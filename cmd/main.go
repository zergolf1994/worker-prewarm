package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"worker-prewarm/internal/config"
	"worker-prewarm/internal/core/logger"
	"worker-prewarm/internal/core/utils"
	"worker-prewarm/internal/dashboard"
	"worker-prewarm/internal/db/database"
	"worker-prewarm/internal/prewarm"
	"worker-prewarm/internal/queue"
)

// version ถูกฝังตอน build โดย GitHub Actions: -ldflags="-X main.version=v1.x.x"
var version = "dev"

func main() {
	config.Load()
	workerID := utils.GenerateWorkerID()
	log.Printf("🚀 Starting Worker Prewarm %s [Worker: %s]", version, workerID)

	// ── Rotating file logger ──────────────────────────────────
	logCloser, err := logger.Init(config.AppConfig.LogPath)
	if err != nil {
		log.Printf("⚠️ File logging disabled: %v", err)
	} else {
		defer logCloser.Close()
		log.Printf("📝 Logging to: %s", config.AppConfig.LogPath)
	}

	// ── MongoDB ───────────────────────────────────────────────
	if err := database.Connect(); err != nil {
		log.Printf("❌ Failed to connect to MongoDB: %v", err)
		time.Sleep(5 * time.Second) // ให้ log ถูก flush / กัน restart-loop รัวๆ
		os.Exit(1)
	}
	defer database.Disconnect()

	// ── Heartbeat ─────────────────────────────────────────────
	// ctx ยกเลิกเมื่อโดน SIGINT/SIGTERM → heartbeat mark ตัวเอง offline ก่อนจบ
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── POP ───────────────────────────────────────────────────
	// ไม่ตั้ง PREWARM_POP (หรือ "auto") → ตรวจจาก CF-Ray ของโดเมนที่ผ่าน CF
	if config.AppConfig.Pop == "" || config.AppConfig.Pop == "auto" {
		config.AppConfig.Pop = prewarm.DetectPop(ctx)
	}
	log.Printf("🌍 POP: %s", config.AppConfig.Pop)

	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		queue.StartHeartbeat(ctx, workerID)
	}()

	// ── Dashboard realtime (หน้า / + SSE /events) ─────────────
	// แบบ server-prewarm เดิม — ดูรายการที่กำลัง warm สดๆ ราย URL
	go dashboard.Start(config.AppConfig.Port)

	// ── Job loop (blocking จนโดน SIGINT/SIGTERM) ──────────────
	// shutdown ระหว่างทำงาน → loop จะ Release งานคืนคิวให้เอง
	queue.RunLoop(ctx, workerID, prewarm.Run)

	log.Println("🛑 Shutting down...")

	// รอ heartbeat ปิดตัว (mark offline) ให้เสร็จก่อน disconnect DB
	select {
	case <-hbDone:
	case <-time.After(10 * time.Second):
		log.Println("⚠️ Heartbeat shutdown timed out")
	}
}
