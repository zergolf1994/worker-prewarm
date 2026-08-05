package queue

import (
	"context"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"worker-prewarm/internal/config"
	"worker-prewarm/internal/core/enums"
	"worker-prewarm/internal/db/models"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ─── Heartbeat ────────────────────────────────────────────────
//
// Upserts this worker into the workers collection every minute, keyed by
// workerId ("prewarm_hostname@n"). The enqueuer (vdohide-service) counts
// capacity from workers with enable=true + fresh heartbeatAt. The enable flag
// is controlled by the admin; heartbeat only initializes it on first insert.

const (
	heartbeatInterval = 1 * time.Minute
	// disk >= 90% → paused + enable=false: keeps beating (visible in admin)
	// but the enqueuer stops counting it as capacity
	diskPauseThreshold = 90.0
)

// StartHeartbeat sends heartbeats until ctx is cancelled, then marks the
// worker offline. Run in a goroutine.
func StartHeartbeat(ctx context.Context, workerID string) {
	log.Printf("💓 Starting heartbeat (every %s, workerId=%s)", heartbeatInterval, workerID)

	workerType, hostname := parseWorkerID(workerID)
	ip := getOutboundIP()
	pid := os.Getpid()

	beat := func() {
		hbCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Slot truth lives in prewarm_queue, not this counter — but report
		// it anyway for the admin dashboard.
		activeJobs, _ := models.PrewarmQueueModel.CountDocuments(hbCtx, bson.M{
			"workerId": workerID,
			"status":   "processing",
		})

		status := enums.WorkerStatusIdle
		if activeJobs > 0 {
			status = enums.WorkerStatusBusy
		}

		// prewarm ไม่เขียนดิสก์ — วัด cwd แค่ให้ admin เห็นสุขภาพเครื่อง
		sys := gatherSystemInfo(".")

		diskPaused := false
		if sys.DiskTotal > 0 {
			diskPct := float64(sys.DiskUsed) / float64(sys.DiskTotal) * 100
			if diskPct >= diskPauseThreshold {
				status = enums.WorkerStatusPaused
				diskPaused = true
				log.Printf("⚠️ Heartbeat: disk usage %.1f%% >= %.0f%% — enable=false", diskPct, diskPauseThreshold)
			}
		}

		now := time.Now()
		setFields := bson.M{
			"hostname": hostname,
			"ip":       ip,
			"pid":      pid,
			"type":     workerType,
			// enqueuer จัดคิวราย pop + ประทับ target ให้ worker ที่ผูก storage
			"pop":        config.AppConfig.Pop,
			"storageId":  config.AppConfig.StorageId,
			"status":     status,
			"activeJobs": activeJobs,
			// bi-level slots: new + reprewarm รวมกัน (ตาม setting prewarm)
			"maxJobs":     MaxJobs(hbCtx),
			"system":      sys,
			"heartbeatAt": now,
			"updatedAt":   now,
		}
		setOnInsert := bson.M{
			"_id":       uuid.New().String(),
			"enable":    true,
			"createdAt": now,
		}
		if diskPaused {
			// A safety pause may disable a worker, but heartbeat must never
			// re-enable it automatically after the disk recovers.
			setFields["enable"] = false
			delete(setOnInsert, "enable")
		}
		update := bson.M{
			"$set":         setFields,
			"$setOnInsert": setOnInsert,
		}
		opts := options.Update().SetUpsert(true)
		if _, err := models.WorkerModel.Col().UpdateOne(hbCtx, bson.M{"workerId": workerID}, update, opts); err != nil {
			log.Printf("⚠️ Heartbeat failed: %v", err)
		}
	}

	beat() // first beat immediately

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			markOffline(workerID)
			return
		case <-ticker.C:
			beat()
		}
	}
}

// markOffline flags the worker offline on graceful shutdown so the admin
// sees it immediately instead of waiting for the heartbeat TTL to expire.
// It deliberately preserves enable because that flag is controlled by admin,
// and expires heartbeatAt so enqueuers stop counting it immediately.
func markOffline(workerID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	now := time.Now()
	_, err := models.WorkerModel.Col().UpdateOne(ctx,
		bson.M{"workerId": workerID},
		bson.M{"$set": bson.M{
			"status":      enums.WorkerStatusOffline,
			"heartbeatAt": time.Unix(0, 0),
			"updatedAt":   now,
		}},
	)
	if err != nil {
		log.Printf("⚠️ Failed to mark worker offline: %v", err)
	} else {
		log.Printf("💤 Worker marked offline (workerId=%s)", workerID)
	}
}

// parseWorkerID splits "type_hostname@n" into worker type and hostname.
// Supports legacy "hostname@n" (type defaults to prewarm).
func parseWorkerID(wID string) (workerType, hostname string) {
	prefix := strings.SplitN(wID, "@", 2)[0]

	workerType = enums.ProcessTypePrewarm
	hostname = prefix
	if idx := strings.Index(prefix, "_"); idx >= 0 {
		workerType = prefix[:idx]
		hostname = prefix[idx+1:]
	}
	return
}

// getOutboundIP returns the preferred outbound IP of this machine.
func getOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			return "unknown"
		}
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
		return "unknown"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}

// DiskUsage exposes disk stats to other packages (disk safety check).
func DiskUsage(path string) (total, used, free int64) {
	return getDiskUsage(path)
}

// gatherSystemInfo collects disk/memory/CPU metrics (per-OS implementations
// in sysinfo_*.go). Disk is measured at the storage path — that's the disk
// that fills up, not necessarily "/".
func gatherSystemInfo(storagePath string) *models.WorkerSystemInfo {
	info := &models.WorkerSystemInfo{}
	info.DiskTotal, info.DiskUsed, info.DiskFree = getDiskUsage(storagePath)
	info.MemTotal, info.MemUsed = getMemoryUsage()
	info.CPUPercent = getCPUPercent()
	return info
}
