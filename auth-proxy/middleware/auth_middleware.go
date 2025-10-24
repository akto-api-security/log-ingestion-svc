package middleware

import (
	"context"
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
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			claims, err := validator.Validate(r.Context(), parts[1])
			if err != nil {
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}

			ctx := context.WithValue(r.Context(), ClaimsContextKey, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
