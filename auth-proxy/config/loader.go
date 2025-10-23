package config

import (
	"os"
)

type Loader struct {
	envFile string
}

func NewLoader(envFile string) *Loader {
	return &Loader{envFile: envFile}
}

func (l *Loader) Load() (*Config, error) {
	config := &Config{
		Port:               getEnv("PORT", "8081"),
		ElasticsearchURL:   getEnv("ELASTICSEARCH_URL", "http://elasticsearch:9200"),
		JWTPublicKey:       getEnv("RSA_PUBLIC_KEY", ""),
		InsecureSkipVerify: getEnv("INSECURE_SKIP_VERIFY", "false") == "true",
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return config, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
