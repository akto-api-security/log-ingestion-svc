package auth

import (
	"context"
	"crypto/rsa"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type JWTValidator struct {
	publicKey  *rsa.PublicKey
	skipVerify bool
}

func NewJWTValidator(publicKeyPEM string, skipVerify bool) (*JWTValidator, error) {
	validator := &JWTValidator{skipVerify: skipVerify}

	if publicKeyPEM != "" {
		pemBytes := normalizePEM(publicKeyPEM)
		publicKey, err := jwt.ParseRSAPublicKeyFromPEM(pemBytes)
		if err != nil {
			if data, err2 := os.ReadFile(publicKeyPEM); err2 == nil {
				publicKey, err = jwt.ParseRSAPublicKeyFromPEM(data)
			}
		}
		if publicKey == nil || err != nil {
			return nil, fmt.Errorf("failed to parse public key: %w", err)
		}
		validator.publicKey = publicKey
	} else {
		return nil, fmt.Errorf("either public key or public key path must be provided")
	}

	return validator, nil
}

func (v *JWTValidator) Validate(ctx context.Context, tokenString string) (*Claims, error) {
	type CustomClaims struct {
		AccountID int64 `json:"accountId"`
		jwt.RegisteredClaims
	}

	if v.skipVerify {
		token, _, err := new(jwt.Parser).ParseUnverified(tokenString, &CustomClaims{})
		if err != nil {
			return nil, fmt.Errorf("failed to parse token: %w", err)
		}
		customClaims, ok := token.Claims.(*CustomClaims)
		if !ok {
			return nil, fmt.Errorf("invalid claims type")
		}
		if customClaims.AccountID == 0 {
			return nil, fmt.Errorf("accountId not found in token")
		}
		return &Claims{
			AccountID: customClaims.AccountID,
			Issuer:    customClaims.Issuer,
			Subject:   customClaims.Subject,
		}, nil
	}

	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		if v.publicKey != nil {
			if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return v.publicKey, nil
		}
		return nil, nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	if !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}

	customClaims, ok := token.Claims.(*CustomClaims)
	if !ok {
		return nil, fmt.Errorf("invalid claims type")
	}

	if customClaims.ExpiresAt != nil && customClaims.ExpiresAt.Before(time.Now()) {
		return nil, fmt.Errorf("token expired")
	}

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
