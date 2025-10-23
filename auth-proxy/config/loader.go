package config

import (
	"os"
	"strings"
)

type Loader struct {
	envFile string
}

func NewLoader(envFile string) *Loader {
	return &Loader{envFile: envFile}
}

func (l *Loader) Load() (*Config, error) {
	l.loadEnvFile()
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

func (l *Loader) loadEnvFile() {
	if l.envFile == "" {
		return
	}
	data, err := os.ReadFile(l.envFile)
	if err != nil {
		return
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			value = value[1 : len(value)-1]
		}
		if os.Getenv(key) == "" {
			os.Setenv(key, value)
		}
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
