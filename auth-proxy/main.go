package main

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Config holds the application configuration
type Config struct {
	Port                string
	FluentdEndpoint     string
	ElasticsearchURL    string
	JWTSecret           string
	JWTPublicKeyPath    string
	AllowedIssuers      []string
	AllowedAudiences    []string
	UseElasticsearch    bool
	InsecureSkipVerify  bool // For testing only - skips signature verification
}

// CustomClaims extends jwt.RegisteredClaims with our custom fields
type CustomClaims struct {
	CustomerID string `json:"customer_id"`
	AccountID  int64  `json:"accountId"` // Support Akto-style accountId
	jwt.RegisteredClaims
}

// GetCustomerID returns the customer identifier (tries customer_id first, then accountId)
func (c *CustomClaims) GetCustomerID() string {
	if c.CustomerID != "" {
		return c.CustomerID
	}
	if c.AccountID != 0 {
		return fmt.Sprintf("%d", c.AccountID)
	}
	return ""
}

// AuthProxy represents the authentication proxy server
type AuthProxy struct {
	config        Config
	publicKey     *rsa.PublicKey
	secretKey     []byte
	keyCache      sync.Map
	httpClient    *http.Client
}

// LogPayload represents the incoming log data
type LogPayload struct {
	Logs []map[string]interface{} `json:"logs"`
}

// NewAuthProxy creates a new AuthProxy instance
func NewAuthProxy(config Config) (*AuthProxy, error) {
	proxy := &AuthProxy{
		config: config,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}

	// Load JWT verification key
	if config.JWTPublicKeyPath != "" {
		key, err := os.ReadFile(config.JWTPublicKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read public key: %w", err)
		}
		publicKey, err := jwt.ParseRSAPublicKeyFromPEM(key)
		if err != nil {
			return nil, fmt.Errorf("failed to parse public key: %w", err)
		}
		proxy.publicKey = publicKey
	} else if config.JWTSecret != "" {
		proxy.secretKey = []byte(config.JWTSecret)
	} else {
		return nil, fmt.Errorf("either JWT_SECRET or JWT_PUBLIC_KEY_PATH must be provided")
	}

	return proxy, nil
}

// validateJWT validates the JWT token and returns the custom claims
func (ap *AuthProxy) validateJWT(tokenString string) (*CustomClaims, error) {
	// Parse without verification if insecure mode enabled (testing only)
	if ap.config.InsecureSkipVerify {
		token, _, err := new(jwt.Parser).ParseUnverified(tokenString, &CustomClaims{})
		if err != nil {
			return nil, fmt.Errorf("failed to parse token: %w", err)
		}
		claims, ok := token.Claims.(*CustomClaims)
		if !ok {
			return nil, fmt.Errorf("invalid claims type")
		}
		
		// Still validate customer_id or accountId exists
		customerID := claims.GetCustomerID()
		if customerID == "" {
			return nil, fmt.Errorf("customer_id or accountId not found in token")
		}
		
		log.Printf("WARNING: Insecure mode - skipping signature verification")
		return claims, nil
	}
	
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		// Verify signing method
		if ap.publicKey != nil {
			if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return ap.publicKey, nil
		}
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return ap.secretKey, nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	if !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}

	claims, ok := token.Claims.(*CustomClaims)
	if !ok {
		return nil, fmt.Errorf("invalid claims type")
	}

	// Validate standard claims
	if claims.ExpiresAt != nil && claims.ExpiresAt.Before(time.Now()) {
		return nil, fmt.Errorf("token expired")
	}

	// Validate issuer if configured
	if len(ap.config.AllowedIssuers) > 0 {
		issuerValid := false
		for _, allowedIssuer := range ap.config.AllowedIssuers {
			if claims.Issuer == allowedIssuer {
				issuerValid = true
				break
			}
		}
		if !issuerValid {
			return nil, fmt.Errorf("invalid issuer: %s", claims.Issuer)
		}
	}

	// Validate audience if configured
	// Only validate if we have allowed audiences configured AND the token has an audience claim
	if len(ap.config.AllowedAudiences) > 0 && len(claims.Audience) > 0 {
		audienceValid := false
		for _, allowedAudience := range ap.config.AllowedAudiences {
			for _, aud := range claims.Audience {
				if aud == allowedAudience {
					audienceValid = true
					break
				}
			}
			if audienceValid {
				break
			}
		}
		if !audienceValid {
			return nil, fmt.Errorf("invalid audience")
		}
	}

	// Validate customer_id or accountId exists
	customerID := claims.GetCustomerID()
	if customerID == "" {
		return nil, fmt.Errorf("customer_id or accountId not found in token")
	}

	return claims, nil
}

// extractToken extracts the JWT token from the Authorization header
func extractToken(r *http.Request) (string, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return "", fmt.Errorf("authorization header missing")
	}

	parts := strings.Split(authHeader, " ")
	if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
		return "", fmt.Errorf("invalid authorization header format")
	}

	return parts[1], nil
}

// forwardToFluentd forwards the log payload to Fluentd
func (ap *AuthProxy) forwardToFluentd(ctx context.Context, customerID string, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, "POST", ap.config.FluentdEndpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Customer-ID", customerID)

	resp, err := ap.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to forward to fluentd: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("fluentd returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// forwardToElasticsearch forwards the log payload to Elasticsearch
func (ap *AuthProxy) forwardToElasticsearch(ctx context.Context, customerID string, payload []byte) error {
	// Parse the payload to inject customer_id
	var logs []map[string]interface{}
	if err := json.Unmarshal(payload, &logs); err != nil {
		return fmt.Errorf("failed to parse logs: %w", err)
	}

	// Inject customer_id into each log entry
	for i := range logs {
		logs[i]["customer_id"] = customerID
		logs[i]["@timestamp"] = time.Now().Format(time.RFC3339)
	}

	// Index each log entry
	for _, logEntry := range logs {
		logJSON, err := json.Marshal(logEntry)
		if err != nil {
			log.Printf("Failed to marshal log entry: %v", err)
			continue
		}

		indexName := fmt.Sprintf("account-%s-logs-%s", customerID, time.Now().Format("2006.01.02"))
		url := fmt.Sprintf("%s/%s/_doc", ap.config.ElasticsearchURL, indexName)

		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(logJSON))
		if err != nil {
			log.Printf("Failed to create ES request: %v", err)
			continue
		}

		req.Header.Set("Content-Type", "application/json")

		resp, err := ap.httpClient.Do(req)
		if err != nil {
			log.Printf("Failed to send to Elasticsearch: %v", err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(resp.Body)
			log.Printf("Elasticsearch returned status %d: %s", resp.StatusCode, string(body))
		}
	}

	return nil
}

// handleLogs handles incoming log requests
func (ap *AuthProxy) handleLogs(w http.ResponseWriter, r *http.Request) {
	// Only accept POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract and validate JWT token
	tokenString, err := extractToken(r)
	if err != nil {
		log.Printf("Token extraction failed: %v", err)
		http.Error(w, "Unauthorized: "+err.Error(), http.StatusUnauthorized)
		return
	}

	claims, err := ap.validateJWT(tokenString)
	if err != nil {
		log.Printf("JWT validation failed: %v", err)
		http.Error(w, "Forbidden: "+err.Error(), http.StatusForbidden)
		return
	}

	customerID := claims.GetCustomerID()
	log.Printf("Authenticated request from customer: %s", customerID)

	// Read the request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Failed to read request body: %v", err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Forward to appropriate backend
	ctx := r.Context()
	if ap.config.UseElasticsearch {
		if err := ap.forwardToElasticsearch(ctx, customerID, body); err != nil {
			log.Printf("Failed to forward to Elasticsearch: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	} else {
		if err := ap.forwardToFluentd(ctx, customerID, body); err != nil {
			log.Printf("Failed to forward to Fluentd: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"success"}`))
}

// handleHealth handles health check requests
func (ap *AuthProxy) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"healthy"}`))
}

// Start starts the HTTP server
func (ap *AuthProxy) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/logs", ap.handleLogs)
	mux.HandleFunc("/health", ap.handleHealth)

	// Add middleware for logging
	handler := loggingMiddleware(mux)

	server := &http.Server{
		Addr:         ":" + ap.config.Port,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("Starting auth proxy on port %s", ap.config.Port)
	return server.ListenAndServe()
}

// loggingMiddleware logs incoming requests
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		log.Printf("%s %s %s", r.Method, r.RequestURI, r.RemoteAddr)
		next.ServeHTTP(w, r)
		log.Printf("Completed in %v", time.Since(start))
	})
}

func main() {
	// Load configuration from environment variables
	config := Config{
		Port:                getEnv("PORT", "8080"),
		FluentdEndpoint:     getEnv("FLUENTD_ENDPOINT", "http://fluentd:8888"),
		ElasticsearchURL:    getEnv("ELASTICSEARCH_URL", "http://elasticsearch:9200"),
		JWTSecret:           getEnv("JWT_SECRET", ""),
		JWTPublicKeyPath:    getEnv("JWT_PUBLIC_KEY_PATH", ""),
		AllowedIssuers:      splitAndFilter(getEnv("ALLOWED_ISSUERS", "akto-auth"), ","),
		AllowedAudiences:    splitAndFilter(getEnv("ALLOWED_AUDIENCES", "akto-logs"), ","),
		UseElasticsearch:    getEnv("USE_ELASTICSEARCH", "true") == "true",
		InsecureSkipVerify:  getEnv("INSECURE_SKIP_VERIFY", "false") == "true",
	}

	proxy, err := NewAuthProxy(config)
	if err != nil {
		log.Fatalf("Failed to create auth proxy: %v", err)
	}

	if err := proxy.Start(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// splitAndFilter splits a string by delimiter and filters out empty strings
func splitAndFilter(s, delimiter string) []string {
	if s == "" {
		return []string{}
	}
	parts := strings.Split(s, delimiter)
	result := []string{}
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
