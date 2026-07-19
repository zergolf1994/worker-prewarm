package database

import (
	"log"

	"worker-prewarm/internal/config"

	"github.com/zergolf1994/goose"
)

// Connect establishes the MongoDB connection via goose ODM.
// DB name comes from the connection string (DATABASE_URL).
//
// Indexes are NOT managed here — the vdohide-service (mongoose) side owns
// all index definitions for shared collections. Creating them from two
// codebases is how stale-index bugs happen.
func Connect() error {
	return goose.Connect(config.AppConfig.MongoURI)
}

// Disconnect closes the MongoDB connection.
func Disconnect() {
	if goose.Client() != nil {
		if err := goose.Close(); err != nil {
			log.Printf("⚠️ Error disconnecting from MongoDB: %v", err)
		} else {
			log.Println("🔌 Disconnected from MongoDB")
		}
	}
}
