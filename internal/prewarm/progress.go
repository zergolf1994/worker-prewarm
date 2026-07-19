package prewarm

import (
	"context"
	"fmt"
	"time"

	"worker-prewarm/internal/core/enums"
	"worker-prewarm/internal/db/models"

	"go.mongodb.org/mongo-driver/bson"
)

// ─── Step-only DB writes ─────────────────────────────────────
// timeline 2 step: collect (แตก URL) → warm (ยิง HEAD ทั้งชุด)
// ระหว่าง warm อัพเดต overallPercent เป็นช่วงๆ (ทุก ~10%) ไม่เขียนถี่

var stepPercent = map[string]float64{
	"collect": 15,
	"warm":    100,
}

func startStep(ctx context.Context, processID, step string) {
	now := time.Now()
	models.VideoProcessModel.UpdateByID(ctx, processID, bson.M{"$set": bson.M{
		fmt.Sprintf("timeline.%s.status", step):    enums.StepStatusProcessing,
		fmt.Sprintf("timeline.%s.percent", step):   0,
		fmt.Sprintf("timeline.%s.startedAt", step): now,
	}})
}

func completeStep(ctx context.Context, processID, step string) {
	now := time.Now()
	set := bson.M{
		fmt.Sprintf("timeline.%s.status", step):  enums.StepStatusCompleted,
		fmt.Sprintf("timeline.%s.percent", step): 100,
		fmt.Sprintf("timeline.%s.endedAt", step): now,
	}
	if pct, ok := stepPercent[step]; ok {
		set["overallPercent"] = pct
	}
	models.VideoProcessModel.UpdateByID(ctx, processID, bson.M{"$set": set})
}

// warmProgress คืน callback สำหรับ Engine.Warm — เขียน DB ทุกก้าว ~10%
// (percent วิ่ง 15 → 95; Complete ปิดที่ 100 เอง)
func warmProgress(processID string) func(done, total int64) {
	var lastStep int64 = -1
	return func(done, total int64) {
		if total <= 0 {
			return
		}
		step := done * 10 / total // 0..10
		if step == lastStep && done != total {
			return
		}
		lastStep = step

		pct := 15 + float64(done)/float64(total)*80
		upCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		models.VideoProcessModel.UpdateByID(upCtx, processID, bson.M{"$set": bson.M{
			"overallPercent":        pct,
			"timeline.warm.percent": float64(done) / float64(total) * 100,
			"timeline.warm.current": done,
			"timeline.warm.total":   total,
		}})
	}
}
