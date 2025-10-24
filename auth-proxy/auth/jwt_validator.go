package auth

import (
	"context"
	"crypto/rsa"
	"fmt"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

type JWTValidator struct {
	publicKey *rsa.PublicKey
}

func NewJWTValidator(publicKeyPEM string) (*JWTValidator, error) {
	if publicKeyPEM == "" {
		return nil, fmt.Errorf("public key must be provided")
	}

	pemBytes := normalizePEM(publicKeyPEM)
	publicKey, err := jwt.ParseRSAPublicKeyFromPEM(pemBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key: %w", err)
	}

	return &JWTValidator{publicKey: publicKey}, nil
}

// Validate parses and validates a JWT token using RSA signature verification.
// It extracts the accountId claim and returns it along with issuer and subject.
func (v *JWTValidator) Validate(ctx context.Context, tokenString string) (*Claims, error) {
	type CustomClaims struct {
		AccountID int64 `json:"accountId"`
		jwt.RegisteredClaims
	}

	// Parse token with RSA signature verification
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		// Verify the signing method is RSA
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return v.publicKey, nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	if !token.Valid {
		return nil, fmt.Errorf("token is invalid")
	}

	// Extract claims
	customClaims, ok := token.Claims.(*CustomClaims)
	if !ok {
		return nil, fmt.Errorf("invalid claims type")
	}

	// Validate accountId exists
	if customClaims.AccountID == 0 {
		return nil, fmt.Errorf("accountId not found in token")
	}

	return &Claims{
		AccountID: customClaims.AccountID,
		Issuer:    customClaims.Issuer,
		Subject:   customClaims.Subject,
	}, nil
}

func normalizePEM(s string) []byte {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	s = strings.ReplaceAll(s, "\\n", "\n")
	const begin = "-----BEGIN PUBLIC KEY-----"
	const end = "-----END PUBLIC KEY-----"
	if strings.Contains(s, begin) && strings.Contains(s, end) {
		s = strings.ReplaceAll(s, begin, begin+"\n")
		s = strings.ReplaceAll(s, end, "\n"+end)
		s = strings.ReplaceAll(s, "\n\n", "\n")
	}
	return []byte(s)
}
