package main

import (
	"log"

	"auth-proxy/auth"
	"auth-proxy/config"
	"auth-proxy/server"
	"auth-proxy/storage"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	validator, err := auth.NewJWTValidator(cfg.JWTPublicKey)
	if err != nil {
		log.Fatalf("Failed to create validator: %v", err)
	}

	logStorage := storage.NewElasticsearchStorage(cfg.ElasticsearchURL)

	srv := server.New(cfg, validator, logStorage)

	if err := srv.Start(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
