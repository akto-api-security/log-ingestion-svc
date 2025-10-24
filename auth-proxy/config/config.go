package config

import (
	"fmt"
	"os"
)

// Config holds the application configuration
type Config struct {
	Port             string
	ElasticsearchURL string
	JWTPublicKey     string
}

// Load reads configuration from environment variables
func Load() (*Config, error) {
	config := &Config{
		Port:             getEnv("PORT", "8081"),
		ElasticsearchURL: getEnv("ELASTICSEARCH_URL", "http://elasticsearch:9200"),
		JWTPublicKey:     getEnv("RSA_PUBLIC_KEY", ""),
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return config, nil
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.Port == "" {
		return fmt.Errorf("PORT is required")
	}
	if c.ElasticsearchURL == "" {
		return fmt.Errorf("ELASTICSEARCH_URL is required")
	}
	if c.JWTPublicKey == "" {
		return fmt.Errorf("RSA_PUBLIC_KEY must be provided")
	}
	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
