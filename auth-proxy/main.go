package main

import (
	"log"

	"auth-proxy/auth"
	"auth-proxy/config"
	"auth-proxy/server"
	"auth-proxy/storage"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load() // loads .env if present, ignore error
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
