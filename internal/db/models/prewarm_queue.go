package models

import (
	"time"

	"github.com/zergolf1994/goose"
)

// PrewarmQueue = งานค้างในคิว prewarm (แยกจาก video_process)
// Collection: "prewarm_queue" | _id: String (UUID)
//
// วงจรชีวิต: enqueuer (vdohide-service) insert pending → worker claim เป็น
// processing → warm เสร็จบันทึกผลลง medias.prewarm.{pop} แล้ว "ลบ doc ทิ้ง"
// — สถานะถาวรอยู่บน media ไม่ใช่ที่นี่
type PrewarmQueue struct {
	ID        string  `bson:"_id" json:"id" goose:"required,default:uuid"`
	MediaID   string  `bson:"mediaId" json:"mediaId" goose:"required,ref:medias"`
	FileID    *string `bson:"fileId,omitempty" json:"fileId,omitempty" goose:"ref:files,index"`
	Slug      *string `bson:"slug,omitempty" json:"slug,omitempty"`           // file slug
	MediaSlug *string `bson:"mediaSlug,omitempty" json:"mediaSlug,omitempty"` // media slug

	Type       *string `bson:"type,omitempty" json:"type,omitempty"` // video | thumbnail
	Resolution *string `bson:"resolution,omitempty" json:"resolution,omitempty"`
	Pop        string  `bson:"pop" json:"pop" goose:"required"`
	Kind       *string `bson:"kind,omitempty" json:"kind,omitempty"` // new | reprewarm

	// เฉพาะงาน new ที่ storage ของ media มี worker ผูกอยู่ — worker ที่ตั้ง
	// STORAGE_ID จะ claim งาน new เฉพาะของ storage ตัวเอง
	TargetStorageID *string `bson:"targetStorageId,omitempty" json:"targetStorageId,omitempty"`

	Status      *string    `bson:"status,omitempty" json:"status,omitempty"` // pending | processing
	WorkerID    *string    `bson:"workerId,omitempty" json:"workerId,omitempty"`
	ClaimedAt   *time.Time `bson:"claimedAt,omitempty" json:"claimedAt,omitempty"`
	NextRetryAt *time.Time `bson:"nextRetryAt,omitempty" json:"nextRetryAt,omitempty"`
	Error       *string    `bson:"error,omitempty" json:"error,omitempty"`
	RetryCount  *int       `bson:"retryCount,omitempty" json:"retryCount,omitempty"`

	CreatedAt time.Time `bson:"createdAt" json:"createdAt" goose:"default:now"`
	UpdatedAt time.Time `bson:"updatedAt" json:"updatedAt" goose:"default:now"`
}

// PrewarmQueueModel is the goose model for the "prewarm_queue" collection.
var PrewarmQueueModel = goose.NewModel[PrewarmQueue]("prewarm_queue")
