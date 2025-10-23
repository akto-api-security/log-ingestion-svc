package middleware

import (
	"context"
	"log"
	"net/http"
	"strings"

	"auth-proxy/auth"
)

type contextKey string

const ClaimsContextKey = contextKey("claims")

func AuthMiddleware(validator auth.Validator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				log.Printf("Authorization header missing")
				http.Error(w, "Unauthorized: authorization header missing", http.StatusUnauthorized)
				return
			}

			parts := strings.Split(authHeader, " ")
			if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
				log.Printf("Invalid authorization header format")
				http.Error(w, "Unauthorized: invalid authorization header format", http.StatusUnauthorized)
				return
			}

			claims, err := validator.Validate(r.Context(), parts[1])
			if err != nil {
				log.Printf("JWT validation failed: %v", err)
				http.Error(w, "Forbidden: "+err.Error(), http.StatusForbidden)
				return
			}

			ctx := context.WithValue(r.Context(), ClaimsContextKey, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
