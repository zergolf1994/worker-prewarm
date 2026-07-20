package prewarm

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"worker-prewarm/internal/core/enums"
	"worker-prewarm/internal/db/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ─── Settings ────────────────────────────────────────────────

// getSettingString อ่านค่า setting เป็น string ("" = ไม่ตั้ง/ไม่มี doc)
func getSettingString(ctx context.Context, name string) string {
	setting, err := models.SettingModel.FindOne(ctx, bson.M{"name": name})
	if err != nil {
		return ""
	}
	return strings.TrimSpace(setting.GetString(""))
}

// normalizeDomain เติม https:// และตัด / ท้าย — ค่าใน settings เก็บแบบ
// โดเมนเปล่าๆ ("cdn.example.com") หรือ URL เต็มก็ได้
func normalizeDomain(d string) string {
	d = strings.TrimRight(strings.TrimSpace(d), "/")
	if d == "" {
		return ""
	}
	if !strings.HasPrefix(d, "http") {
		d = "https://" + d
	}
	return d
}

// prewarmParallel อ่านจำนวน HEAD พร้อมกันต่อหนึ่งงานจาก setting "prewarm"
// แยกตามชนิดงาน: new → prewarm_parallel (default 10),
// reprewarm → prewarm_old_parallel (default 20) — key เดิมของระบบเก่า
func prewarmParallel(ctx context.Context, kind string) int {
	key, def := "prewarm_parallel", 10
	if kind == "reprewarm" {
		key, def = "prewarm_old_parallel", 20
	}

	setting, err := models.SettingModel.FindOne(ctx, bson.M{"name": enums.SettingPrewarm})
	if err != nil {
		return def
	}
	cfg, ok := asBsonM(setting.Value)
	if !ok {
		return def
	}
	if v, exists := cfg[key]; exists {
		if n := toInt(v); n > 0 {
			return n
		}
	}
	return def
}

// asBsonM แปลงค่า interface{} ที่อาจ decode มาเป็น bson.M / map / bson.D
// (default registry decode document เป็น bson.D — เจอมาแล้วกับ kill switch)
func asBsonM(v interface{}) (bson.M, bool) {
	switch m := v.(type) {
	case bson.M:
		return m, true
	case map[string]interface{}:
		return bson.M(m), true
	case bson.D:
		out := bson.M{}
		for _, e := range m {
			out[e.Key] = e.Value
		}
		return out, true
	default:
		return nil, false
	}
}

func toInt(v interface{}) int {
	switch val := v.(type) {
	case int:
		return val
	case int32:
		return int(val)
	case int64:
		return int(val)
	case float64:
		return int(val)
	case string:
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return 0
}

// ─── File helpers ────────────────────────────────────────────

// collectCloneSlugs คืน slug ของ cloned files ที่ยัง active — clone ใช้
// medias ชุดเดียวกับต้นฉบับ ต่างกันแค่ master playlist / sprite.vtt ต่อ slug
func collectCloneSlugs(ctx context.Context, fileID string) []string {
	slugs := []string{}
	cursor, err := models.FileModel.FindRaw(ctx, bson.M{
		"clonedFrom":         fileID,
		"type":               enums.FileTypeVideo,
		"metadata.trashedAt": bson.M{"$exists": false},
		"metadata.deletedAt": bson.M{"$exists": false},
	}, options.Find().SetProjection(bson.M{"slug": 1}))
	if err != nil {
		return slugs
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var f models.File
		if err := cursor.Decode(&f); err != nil {
			continue
		}
		if f.Slug != "" {
			slugs = append(slugs, f.Slug)
		}
	}
	return slugs
}

// recordPrewarm บันทึกผลรอบล่าสุดลง medias.prewarm.{pop} (shape เดียวกับ
// ระบบเก่า: {data, prewarmAt}) — enqueuer ใช้ prewarmAt ตัดสิน re-prewarm
// และ pop อื่นใช้ prewarm.fra.prewarmAt เป็นเงื่อนไขเริ่มงาน
// ⚠ ยิงตรงที่ collection — goose FindByIDAndUpdate จะแอบ $set updatedAt
// ให้เอง ซึ่งเราไม่อยากให้การ warm ไปแตะ updatedAt ของ media
func recordPrewarm(ctx context.Context, mediaID, pop string, stats WarmStats) error {
	_, err := models.MediaModel.Col().UpdateOne(ctx,
		bson.M{"_id": mediaID},
		bson.M{
			"$set": bson.M{
				"prewarm." + pop: bson.M{
					"data": bson.M{
						"total":   stats.Total,
						"hit":     stats.Hit,
						"miss":    stats.Miss,
						"expired": stats.Expired,
						"failed":  stats.Failed,
					},
					"prewarmAt": time.Now(),
				},
			},
		},
	)
	if err != nil && errors.Is(err, mongo.ErrNoDocuments) {
		return nil // media หายไประหว่าง warm — ไม่เป็นไร
	}
	return err
}
