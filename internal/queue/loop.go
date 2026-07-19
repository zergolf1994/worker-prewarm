package queue

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"worker-prewarm/internal/core/enums"
	"worker-prewarm/internal/db/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// ─── Job loop ─────────────────────────────────────────────────
//
// resume own processing job (crash recovery) → then loop:
// kill switch / disk gate → Claim → run → Complete | Fail.
// On shutdown mid-job the job is Released back to pending so another
// worker picks it up immediately.

// JobHandler runs one claimed job. It must respect ctx — on cancel it
// should abort quickly and return ctx's error so the loop Releases the
// job instead of marking it failed.
type JobHandler func(ctx context.Context, job *models.VideoProcess) error

const claimInterval = 10 * time.Second // idle poll — queue empty / disabled

// ClaimGate — optional pre-claim check set by main. A non-empty reason
// skips claiming this round (worker's storage disabled/full/offline in
// the DB), so blocked workers idle instead of claim-release hot-looping.
var ClaimGate func(ctx context.Context) string

// RunLoop claims and runs jobs until ctx is cancelled. Blocking — call
// from main after StartHeartbeat is up.
func RunLoop(ctx context.Context, workerID string, handler JobHandler) {
	log.Printf("🔁 Job loop started (poll every %s)", claimInterval)

	// ── Crash recovery: finish our own half-done job first ────
	if job, err := ResumeOwn(ctx, workerID); err != nil {
		log.Printf("⚠️ ResumeOwn failed: %v", err)
	} else if job != nil {
		log.Printf("♻️ Resuming interrupted job %s (file=%s)", job.ID, strPtr(job.FileID))
		runJob(ctx, job, handler)
	}

	for {
		if ctx.Err() != nil {
			log.Println("🔁 Job loop stopped")
			return
		}

		// kill switch (prewarm_config.enabled) — shared with the enqueuer
		if !prewarmEnabled(ctx) {
			sleepCtx(ctx, claimInterval)
			continue
		}

		// optional gate hook (ไม่ใช้ในโหมดปกติ — prewarm ไม่แตะดิสก์/storage)
		if ClaimGate != nil {
			if reason := ClaimGate(ctx); reason != "" {
				logGateReason(reason)
				sleepCtx(ctx, claimInterval)
				continue
			}
			if lastGateReason != "" {
				log.Println("✅ Claiming resumed")
				lastGateReason = ""
			}
		}

		job, err := Claim(ctx, workerID)
		if err != nil {
			// ctx cancel ระหว่าง Claim ก็เข้าทางนี้ — เช็คหัว loop จะจบเอง
			if ctx.Err() == nil {
				log.Printf("⚠️ Claim failed: %v", err)
			}
			sleepCtx(ctx, claimInterval)
			continue
		}
		if job == nil {
			sleepCtx(ctx, claimInterval) // queue empty
			continue
		}

		runJob(ctx, job, handler)
		// no sleep — if there's another pending job, take it right away
	}
}

// cancelPollInterval — ความถี่ที่ watcher เช็คว่า admin กดยกเลิกงานนี้หรือยัง
const cancelPollInterval = 5 * time.Second

// watchCancel เฝ้า video_process ของงานที่กำลังรัน — เห็น status=cancelled
// เมื่อไหร่ก็จุดระเบิด cancelJob → ทุก I/O (HTTP/ffmpeg/S3) ที่ผูก jobCtx ตายทันที
func watchCancel(jobCtx context.Context, cancelJob context.CancelCauseFunc, jobID string) {
	ticker := time.NewTicker(cancelPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-jobCtx.Done():
			return // งานจบเอง / shutdown — เลิกเฝ้า
		case <-ticker.C:
			qCtx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			p, err := models.VideoProcessModel.FindByID(qCtx, jobID)
			cancel()
			if err == nil && p.Status != nil && *p.Status == enums.ProcessStatusCancelled {
				log.Printf("⏹️ Cancel detected for job %s — aborting all I/O now", jobID)
				cancelJob(ErrJobCancelled)
				return
			}
		}
	}
}

// runJob executes one job and settles its final status.
func runJob(ctx context.Context, job *models.VideoProcess, handler JobHandler) {
	log.Printf("▶️ Job %s started (file=%s, slug=%s)", job.ID, strPtr(job.FileID), strPtr(job.Slug))
	start := time.Now()

	// per-job ctx: ตายได้ 2 ทาง — parent (shutdown) หรือ watcher (admin cancel)
	// แยกเหตุด้วย context.Cause ตอน settle
	jobCtx, cancelJob := context.WithCancelCause(ctx)
	defer cancelJob(nil)
	go watchCancel(jobCtx, cancelJob, job.ID)

	err := handler(jobCtx, job)

	// admin cancel: อาจโผล่มาเป็น sentinel ตรงๆ (จุดเช็คใน run) หรือเป็น
	// context.Canceled ที่แทรกอยู่ในความล้มเหลวของ I/O — ดู cause เป็นหลัก
	cancelled := errors.Is(err, ErrJobCancelled) ||
		errors.Is(context.Cause(jobCtx), ErrJobCancelled)

	// settle ด้วย ctx ใหม่เสมอ — ตอน shutdown ctx หลักถูก cancel ไปแล้ว
	// แต่เรายังต้องเขียนสถานะปิดงานให้สำเร็จ
	settleCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	switch {
	case err == nil:
		if e := Complete(settleCtx, job.ID); e != nil {
			log.Printf("⚠️ Complete failed for job %s: %v", job.ID, e)
		}
		log.Printf("✅ Job %s completed in %s", job.ID, time.Since(start).Round(time.Second))

	case cancelled:
		// admin สั่งยกเลิก — doc เป็น cancelled แล้ว ห้ามไปเขียนทับ
		log.Printf("⏹️ Job %s cancelled by admin (after %s)", job.ID, time.Since(start).Round(time.Second))

	case ctx.Err() != nil || errors.Is(err, context.Canceled), errors.Is(err, ErrJobRequeue):
		// shutdown / disk เต็ม — ไม่ใช่ความผิดของงาน คืนเข้าคิวไม่นับ retry
		if e := Release(settleCtx, job.ID); e != nil {
			log.Printf("⚠️ Release failed for job %s: %v", job.ID, e)
		}
		log.Printf("↩️ Job %s released back to queue: %v", job.ID, err)

	default:
		retried, e := RetryOrFail(settleCtx, job, err.Error(), categorize(err))
		if e != nil {
			log.Printf("⚠️ RetryOrFail update failed for job %s: %v", job.ID, e)
		}
		attempt := 1
		if job.RetryCount != nil {
			attempt = *job.RetryCount + 1
		}
		if retried {
			log.Printf("🔄 Job %s failed (attempt %d/%d) — requeued with backoff: %v", job.ID, attempt, MaxRetries, err)
		} else {
			log.Printf("❌ Job %s failed permanently (attempt %d/%d) — file marked error: %v", job.ID, attempt, MaxRetries, err)
		}
	}
}

// ─── Helpers ──────────────────────────────────────────────────

// prewarmEnabled reads prewarm_config.enabled — missing/malformed = true
// (fail-open: a broken settings doc must not silently stop every worker).
func prewarmEnabled(ctx context.Context) bool {
	setting, err := models.SettingModel.FindOne(ctx, bson.M{"name": enums.SettingPrewarmConfig})
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) && ctx.Err() == nil {
			log.Printf("⚠️ Read prewarm_config failed: %v", err)
		}
		return true
	}
	cfg, ok := setting.Value.(bson.M)
	if !ok {
		switch v := setting.Value.(type) {
		case map[string]interface{}:
			cfg = bson.M(v)
		case bson.D:
			// default registry decode document เป็น bson.D ไม่ใช่ bson.M
			cfg = bson.M{}
			for _, e := range v {
				cfg[e.Key] = e.Value
			}
		default:
			return true
		}
	}
	if enabled, ok := cfg["enabled"].(bool); ok {
		return enabled
	}
	return true
}

// categorize maps an error to errorCategory for the admin dashboard.
func categorize(err error) string {
	e := strings.ToLower(err.Error())
	switch {
	case strings.Contains(e, "timeout") || strings.Contains(e, "connection") || strings.Contains(e, "refused"):
		return "network"
	case strings.Contains(e, "playlist") || strings.Contains(e, "m3u8"):
		return "playlist"
	case strings.Contains(e, "setting") || strings.Contains(e, "domain"):
		return "config"
	default:
		return "unknown"
	}
}

// logGateReason logs a claim-gate block, but only when the reason changes —
// a blocked worker polls every 10s and must not spam the log.
var lastGateReason string

func logGateReason(reason string) {
	if reason != lastGateReason {
		log.Printf("⛔ Claiming paused: %s", reason)
		lastGateReason = reason
	}
}

// sleepCtx sleeps for d or until ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func strPtr(s *string) string {
	if s == nil {
		return "-"
	}
	return *s
}
