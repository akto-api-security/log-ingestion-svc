package main

import (
"fmt"
"github.com/golang-jwt/jwt/v5"
)

func main() {
secret := []byte("your-secret-key-change-this-in-production")

// Create simple claims
claims := jwt.MapClaims{
"customer_id": "customer1",
"iss": "akto-auth",
"aud": []string{"akto-logs"},
}

// Create token
token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
tokenString, err := token.SignedString(secret)
if err != nil {
fmt.Printf("Error signing: %v\n", err)
return
}

fmt.Println("Token:", tokenString)

// Now verify it
parsedToken, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
return secret, nil
})

if err != nil {
fmt.Printf("Error parsing: %v\n", err)
return
}

if parsedToken.Valid {
fmt.Println("Token is VALID!")
} else {
fmt.Println("Token is INVALID!")
}
}
