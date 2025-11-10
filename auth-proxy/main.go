package main

import (
	"log"

	"auth-proxy/auth"
	"auth-proxy/config"
	"auth-proxy/server"
	"auth-proxy/storage"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load() //If .env file is present, load it
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	elasticsearchClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{cfg.ElasticsearchURL},
	})
	if err != nil {
		log.Fatalf("Failed to create Elasticsearch client: %v", err)
	}

	response, err := elasticsearchClient.Info()
	if err != nil {
		log.Fatalf("Failed to connect to Elasticsearch: %v", err)
	}
	defer response.Body.Close()

	if response.IsError() {
		log.Fatalf("Elasticsearch returned error status: %s", response.Status())
	}

	log.Printf("Connected to Elasticsearch successfully")

	validator, err := auth.NewJWTValidator(cfg.JWTPublicKey)
	if err != nil {
		log.Fatalf("Failed to create validator: %v", err)
	}

	logStorage := storage.NewElasticsearchStorage(elasticsearchClient)

	srv := server.New(cfg, validator, logStorage)

	if err := srv.Start(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
