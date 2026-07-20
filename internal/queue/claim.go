package queue

import (
	"context"
	"errors"
	"time"

	"worker-prewarm/internal/config"
	"worker-prewarm/internal/db/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ─── Claim ────────────────────────────────────────────────────
//
// enqueuer (vdohide-service) เติม prewarm_queue เป็น pending — worker claim
// แบบ atomic (FindOneAndUpdate pending → processing) เฉพาะงานของ pop ตัวเอง
//
// กติกา storage:
//   - ตั้ง STORAGE_ID → งาน new เฉพาะ targetStorageId = ของตัวเอง
//     + งาน reprewarm ทุกตัว (ไม่ประทับ target)
//   - ไม่ตั้ง → เฉพาะงานที่ไม่ประทับ target (new แบบ pool + reprewarm)

// Claim atomically claims the next pending prewarm job of the given kind
// ("new" | "reprewarm") for this worker — loop เรียกแยกช่องตาม slot ว่าง.
// Returns (nil, nil) when the queue is empty.
func Claim(ctx context.Context, workerID, kind string) (*models.PrewarmQueue, error) {
	now := time.Now()
	filter := bson.M{
		"pop":    config.AppConfig.Pop,
		"status": "pending",
		"kind":   kind,
		// งานที่รอ retry (backoff) ยังไม่ถึงเวลา — ข้ามไว้ก่อน
		"$or": []bson.M{
			{"nextRetryAt": bson.M{"$exists": false}},
			{"nextRetryAt": bson.M{"$lte": now}},
		},
	}
	// งาน new เท่านั้นที่ผูก storage ได้ (reprewarm ไม่ประทับ target — ใครก็หยิบได้)
	if kind == "new" {
		if config.AppConfig.StorageId != "" {
			filter["targetStorageId"] = config.AppConfig.StorageId
		} else {
			filter["targetStorageId"] = bson.M{"$exists": false}
		}
	}

	job, err := models.PrewarmQueueModel.FindOneAndUpdate(ctx,
		filter,
		bson.M{
			"$set": bson.M{
				"status":    "processing",
				"workerId":  workerID,
				"claimedAt": now,
			},
		},
		options.FindOneAndUpdate().
			SetSort(bson.D{{Key: "createdAt", Value: 1}}).
			SetReturnDocument(options.After),
	)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil // queue empty — not an error
		}
		return nil, err
	}
	return job, nil
}

// ─── Job lifecycle ────────────────────────────────────────────

// Complete — งานเสร็จ (ผลถูกบันทึกลง media.prewarm.{pop} แล้ว) → ลบ doc ทิ้ง
// คิวนี้เก็บเฉพาะงานค้าง สถานะถาวรอยู่บน media
func Complete(ctx context.Context, jobID string) error {
	_, err := models.PrewarmQueueModel.Col().DeleteOne(ctx, bson.M{"_id": jobID})
	return err
}

// MaxRetries — a job fails this many times before being dropped.
const MaxRetries = 3

// ErrJobRequeue — failure is not the job's fault (เช่น setting ยังไม่ตั้ง);
// Release back to pending WITHOUT counting a retry.
var ErrJobRequeue = errors.New("job requeue")

// retryBackoff returns the wait before attempt n runs again (1m, 2m, ...).
func retryBackoff(attempt int) time.Duration {
	return time.Duration(1<<(attempt-1)) * time.Minute
}

// RetryOrFail settles a failed run. Under MaxRetries the SAME doc goes back
// to pending with a backoff. At MaxRetries the doc is DROPPED — ผลล้มเหลว
// ถูกบันทึกไว้บน media.prewarm.{pop} โดย handler แล้ว (นับเป็น warm รอบนี้
// จบ ไปรอ reprewarm ตามอายุ) คิวไม่เก็บซาก
func RetryOrFail(ctx context.Context, job *models.PrewarmQueue, errMsg string) (retried bool, err error) {
	attempt := 1
	if job.RetryCount != nil {
		attempt = *job.RetryCount + 1
	}

	if attempt < MaxRetries {
		_, err = models.PrewarmQueueModel.FindByIDAndUpdate(ctx, job.ID, bson.M{
			"$set": bson.M{
				"status":      "pending",
				"error":       errMsg,
				"nextRetryAt": time.Now().Add(retryBackoff(attempt)),
			},
			"$inc":   bson.M{"retryCount": 1},
			"$unset": bson.M{"workerId": "", "claimedAt": ""},
		})
		return true, err
	}

	_, err = models.PrewarmQueueModel.Col().DeleteOne(ctx, bson.M{"_id": job.ID})
	return false, err
}

// Release returns a claimed job to the queue (processing → pending),
// clearing ownership. Called on graceful shutdown.
func Release(ctx context.Context, jobID string) error {
	_, err := models.PrewarmQueueModel.FindOneAndUpdate(ctx,
		bson.M{
			"_id":    jobID,
			"status": "processing",
		},
		bson.M{
			"$set":   bson.M{"status": "pending"},
			"$unset": bson.M{"workerId": "", "claimedAt": ""},
		},
	)
	if err != nil && errors.Is(err, mongo.ErrNoDocuments) {
		return nil // already completed/reaped — nothing to release
	}
	return err
}
