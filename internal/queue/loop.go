package queue

import (
	"context"
	"errors"
	"log"
	"time"

	"worker-prewarm/internal/core/enums"
	"worker-prewarm/internal/db/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// ─── Job loop ─────────────────────────────────────────────────
//
// resume own processing job (crash recovery) → then loop:
// kill switch → Claim → run → Complete (ลบ doc) | Retry | Release.
// On shutdown mid-job the job is Released back to pending so another
// worker picks it up immediately.

// JobHandler runs one claimed job. It must respect ctx — on cancel it
// should abort quickly and return ctx's error so the loop Releases the
// job instead of retrying it.
type JobHandler func(ctx context.Context, job *models.PrewarmQueue) error

const claimInterval = 10 * time.Second // idle poll — queue empty / disabled

// requeueDelay — งานที่คืนคิวเพราะ config ไม่พร้อม (เช่น domain_playlist
// ยังไม่ตั้ง) รอเท่านี้ก่อนถูกหยิบใหม่ กัน claim-release วนรัว
const requeueDelay = 1 * time.Minute

// RunLoop claims and runs jobs until ctx is cancelled. Blocking — call
// from main after StartHeartbeat is up.
func RunLoop(ctx context.Context, workerID string, handler JobHandler) {
	log.Printf("🔁 Job loop started (poll every %s)", claimInterval)

	// ── Crash recovery: finish our own half-done job first ────
	if job, err := ResumeOwn(ctx, workerID); err != nil {
		log.Printf("⚠️ ResumeOwn failed: %v", err)
	} else if job != nil {
		log.Printf("♻️ Resuming interrupted job %s (media=%s)", job.ID, job.MediaID)
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

// runJob executes one job and settles its final status.
func runJob(ctx context.Context, job *models.PrewarmQueue, handler JobHandler) {
	start := time.Now()

	err := handler(ctx, job)

	// settle ด้วย ctx ใหม่เสมอ — ตอน shutdown ctx หลักถูก cancel ไปแล้ว
	// แต่เรายังต้องเขียนสถานะปิดงานให้สำเร็จ
	settleCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	switch {
	case err == nil:
		if e := Complete(settleCtx, job.ID); e != nil {
			log.Printf("⚠️ Complete failed for job %s: %v", job.ID, e)
		}

	case ctx.Err() != nil || errors.Is(err, context.Canceled):
		// shutdown — คืนเข้าคิวทันที ให้ตัวอื่นหยิบต่อ
		if e := Release(settleCtx, job.ID); e != nil {
			log.Printf("⚠️ Release failed for job %s: %v", job.ID, e)
		}
		log.Printf("↩️ Job %s released back to queue: %v", job.ID, err)

	case errors.Is(err, ErrJobRequeue):
		// config ไม่พร้อม (ไม่ใช่ความผิดของงาน) — คืนคิวพร้อมหน่วงเวลา
		if e := ReleaseWithDelay(settleCtx, job.ID, requeueDelay); e != nil {
			log.Printf("⚠️ Release failed for job %s: %v", job.ID, e)
		}
		log.Printf("↩️ Job %s requeued (+%s): %v", job.ID, requeueDelay, err)

	default:
		retried, e := RetryOrFail(settleCtx, job, err.Error())
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
			log.Printf("❌ Job %s failed permanently (attempt %d/%d) — dropped from queue: %v", job.ID, attempt, MaxRetries, err)
		}
	}

	_ = start // duration ถูก log โดย handler เอง (LogMain สรุปต่อชิ้น)
}

// ReleaseWithDelay คืนงานเข้าคิวแบบมี nextRetryAt — ไม่นับ retry
func ReleaseWithDelay(ctx context.Context, jobID string, d time.Duration) error {
	_, err := models.PrewarmQueueModel.FindOneAndUpdate(ctx,
		bson.M{"_id": jobID, "status": "processing"},
		bson.M{
			"$set":   bson.M{"status": "pending", "nextRetryAt": time.Now().Add(d)},
			"$unset": bson.M{"workerId": "", "claimedAt": ""},
		},
	)
	if err != nil && errors.Is(err, mongo.ErrNoDocuments) {
		return nil
	}
	return err
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

// sleepCtx sleeps for d or until ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
