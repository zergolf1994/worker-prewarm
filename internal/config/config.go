package config

import (
	"os"

	"github.com/joho/godotenv"
)

// AppConfig holds the application configuration loaded from environment variables.
var AppConfig Config

// Config represents the application configuration.
type Config struct {
	MongoURI string

	LogPath string // Path to rotating log file (env: LOG_PATH)
}

// Load reads configuration from environment variables (and .env file).
func Load() {
	// Load .env file if present (ignore error if not found)
	godotenv.Load()

	AppConfig = Config{
		MongoURI: getEnv("DATABASE_URL", "mongodb://localhost:27017"),
		LogPath:  getEnv("LOG_PATH", "logs/worker-prewarm.log"),
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
