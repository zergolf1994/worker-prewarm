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

// ErrJobRequeue — failure is not the job's fault (เช่น setting ยังไม่ตั้ง);
// Release back to pending WITHOUT counting a retry.
var ErrJobRequeue = errors.New("job requeue")

// ⚠ ไม่มี retry ในคิวแล้ว — งาน warm ที่ล้มจะถูกบันทึกผลลง
// medias.prewarm.{pop} แล้วลบ doc ทิ้ง ให้ media กลับมาเองตามรอบ reprewarm
// ส่วน error อื่นก็ทิ้งงานไป เพราะ enqueuer จัดคิวใหม่ให้ทุกนาทีอยู่แล้ว
// (งานที่คา nextRetryAt กินโควตาต่อ storage ของ enqueuer ทิ้งไว้เปล่าๆ และ
//  ถ้า worker เจ้าของ targetStorageId ดับ จะค้างถาวรเพราะไม่มีใคร claim)

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
