package config

import "fmt"

// Config holds the application configuration
type Config struct {
	Port               string
	ElasticsearchURL   string
	JWTPublicKey       string
	InsecureSkipVerify bool
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
