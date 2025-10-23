package main

import (
"crypto/rsa"
"crypto/x509"
"encoding/pem"
"fmt"
"log"
"os"
"time"

"github.com/golang-jwt/jwt/v5"
)

// CustomClaims extends jwt.RegisteredClaims with our custom fields
type CustomClaims struct {
AccountID int64 `json:"accountId"`
jwt.RegisteredClaims
}

// GenerateJWT generates a JWT token for an account using RS256
func GenerateJWT(accountID int64, privateKey *rsa.PrivateKey) (string, error) {
claims := CustomClaims{
AccountID: accountID,
RegisteredClaims: jwt.RegisteredClaims{
Issuer:    "akto-auth",
Audience:  []string{"akto-logs"},
ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour * 365)), // 1 year
IssuedAt:  jwt.NewNumericDate(time.Now()),
NotBefore: jwt.NewNumericDate(time.Now()),
},
}

token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
return token.SignedString(privateKey)
}

func main() {
// Load private key
privateKeyData, err := os.ReadFile("private_key.pem")
if err != nil {
log.Fatalf("Failed to read private key: %v", err)
}

block, _ := pem.Decode(privateKeyData)
if block == nil {
log.Fatal("Failed to decode PEM block")
}

privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
if err != nil {
log.Fatalf("Failed to parse private key: %v", err)
}

// Generate tokens for different accounts
accounts := []int64{1000001, 2000000, 3000000}

fmt.Println("Generated RS256 JWT Tokens:")
fmt.Println("=====================")
for _, accountID := range accounts {
token, err := GenerateJWT(accountID, privateKey)
if err != nil {
log.Fatalf("Failed to generate token for %d: %v", accountID, err)
}
fmt.Printf("\nAccount %d:\n%s\n", accountID, token)
}
fmt.Println("\n=====================")
fmt.Println("\nAdd these tokens to your Fluent Bit configuration")
fmt.Println("Set JWT_PUBLIC_KEY_PATH to: public_key.pem")
}
