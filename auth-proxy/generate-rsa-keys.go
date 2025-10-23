package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log"
	"os"
)

func main() {
	// Generate RSA key pair
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		log.Fatal(err)
	}

	// Save private key
	privateKeyFile, err := os.Create("private_key.pem")
	if err != nil {
		log.Fatal(err)
	}
	defer privateKeyFile.Close()

	privateKeyPEM := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	}
	if err := pem.Encode(privateKeyFile, privateKeyPEM); err != nil {
		log.Fatal(err)
	}

	// Save public key
	publicKeyFile, err := os.Create("public_key.pem")
	if err != nil {
		log.Fatal(err)
	}
	defer publicKeyFile.Close()

	publicKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		log.Fatal(err)
	}

	publicKeyPEM := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	}
	if err := pem.Encode(publicKeyFile, publicKeyPEM); err != nil {
		log.Fatal(err)
	}

	fmt.Println("✓ Generated RSA key pair:")
	fmt.Println("  - private_key.pem (keep secret)")
	fmt.Println("  - public_key.pem (share for verification)")
}
