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

	// Port ของ dashboard realtime (หน้า / + /events) — default 8886
	// เหมือน server-prewarm เดิม; ชนกันก็แค่ไม่มี dashboard worker ทำงานต่อ
	Port string

	// Pop = edge location ของเครื่องนี้ (fra, sin, ...) — claim เฉพาะงานของ
	// pop ตัวเอง และบันทึกผลลง medias.prewarm.{pop}
	// ว่าง/"auto" = ตรวจเองตอน start จาก CF-Ray (main เซ็ตค่ากลับเข้ามา)
	Pop string

	// StorageId (optional) — ตั้งแล้ว claim งาน new เฉพาะ media ของ storage นี้
	// (งาน reprewarm หยิบได้เสมอไม่ว่าตั้งหรือไม่)
	StorageId string

	LogPath string // Path to rotating log file (env: LOG_PATH)
}

// Load reads configuration from environment variables (and .env file).
func Load() {
	// Load .env file if present (ignore error if not found)
	godotenv.Load()

	AppConfig = Config{
		MongoURI:  getEnv("DATABASE_URL", "mongodb://localhost:27017"),
		Port:      getEnv("PORT", getEnv("HTTP_PORT", "8886")),
		Pop:       getEnv("PREWARM_POP", ""), // ว่าง = auto-detect
		StorageId: getEnv("STORAGE_ID", ""),
		LogPath:   getEnv("LOG_PATH", "logs/worker-prewarm.log"),
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
