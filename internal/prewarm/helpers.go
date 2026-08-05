package prewarm

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"worker-prewarm/internal/core/enums"
	"worker-prewarm/internal/db/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// ─── Settings ────────────────────────────────────────────────

// settingCacheTTL — อ่าน setting จาก DB ถี่สุดเท่านี้ ค่าพวกนี้แทบไม่เปลี่ยน
// แต่ถูกเรียกทุก job (4 ครั้ง/job × 150 job/นาที = 600 query ที่ไม่จำเป็น)
// ใช้แพตเทิร์นเดียวกับ readPrewarmConfig ใน queue/loop.go
const settingCacheTTL = 10 * time.Second

type settingEntry struct {
	doc *models.Setting
	at  time.Time
}

var (
	settingMu    sync.RWMutex
	settingCache = map[string]settingEntry{}
)

// getSetting คืน setting doc จาก cache — หมดอายุแล้วค่อยยิง DB
// อ่าน DB ไม่ได้แต่มีค่าเดิมอยู่ → ใช้ค่าเดิมต่อ ดีกว่าให้งานล้มทั้งชุด
func getSetting(ctx context.Context, name string) *models.Setting {
	settingMu.RLock()
	entry, cached := settingCache[name]
	settingMu.RUnlock()

	if cached && time.Since(entry.at) < settingCacheTTL {
		return entry.doc
	}

	doc, err := models.SettingModel.FindOne(ctx, bson.M{"name": name})
	if err != nil {
		if cached {
			return entry.doc
		}
		return nil
	}

	settingMu.Lock()
	settingCache[name] = settingEntry{doc: doc, at: time.Now()}
	settingMu.Unlock()
	return doc
}

// getSettingString อ่านค่า setting เป็น string ("" = ไม่ตั้ง/ไม่มี doc)
func getSettingString(ctx context.Context, name string) string {
	setting := getSetting(ctx, name)
	if setting == nil {
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

// normalizeReferer returns a page-like URL. A bare origin without the trailing
// slash (for example, https://fembed.co) is a different string from the
// browser-style Referer https://fembed.co/ and may not match Cloudflare rules
// generated from the configured preview domain.
func normalizeReferer(d string) string {
	d = normalizeDomain(d)
	if d == "" {
		return ""
	}
	return d + "/"
}

// prewarmParallel อ่านจำนวน HEAD พร้อมกันต่อหนึ่งงานจาก setting "prewarm"
// แยกตามชนิดงาน: new → prewarm_parallel (default 10),
// reprewarm → prewarm_old_parallel (default 20) — key เดิมของระบบเก่า
func prewarmParallel(ctx context.Context, kind string) int {
	key, def := "prewarm_parallel", 10
	if kind == "reprewarm" {
		key, def = "prewarm_old_parallel", 20
	}

	setting := getSetting(ctx, enums.SettingPrewarm)
	if setting == nil {
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

// maxFailPercent อ่าน prewarm.max_fail_percent (default 10)
// ยังไม่ได้ใช้ตัดสินอะไรตอนนี้ (ล้มเท่าไหร่ก็บันทึกผลไปเลย ไม่ retry)
// เก็บไว้เผื่อวันหลังอยากมีเกณฑ์ เช่น "ล้มเกิน x% ไม่ต้องนับเป็น warm สำเร็จ"
//
//nolint:unused // เก็บไว้ใช้ภายหลัง
func maxFailPercent(ctx context.Context) int {
	setting := getSetting(ctx, enums.SettingPrewarm)
	if setting == nil {
		return 10
	}
	cfg, ok := asBsonM(setting.Value)
	if !ok {
		return 10
	}
	if v, exists := cfg["max_fail_percent"]; exists {
		if n := toInt(v); n > 0 {
			return n
		}
	}
	return 10
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

// JobMeta คือข้อมูลที่ต้องใช้ประกอบ URL — ปกติมาจาก queue doc ตรงๆ
type JobMeta struct {
	MediaSlug  string
	FileSlug   string
	Type       string
	Resolution string
}

// resolveJobMeta อ่านค่าจาก queue doc; ถ้าไม่ครบ (doc เก่าที่ enqueue ไว้
// ก่อนเวอร์ชันนี้) ค่อย fallback ไปดึง DB แบบเดิม — คืน nil ถ้างานนี้ไม่มี
// อะไรให้ทำแล้ว (media/file หายไป)
func resolveJobMeta(ctx context.Context, job *models.PrewarmQueue) (*JobMeta, error) {
	meta := &JobMeta{
		MediaSlug:  strVal(job.MediaSlug),
		FileSlug:   strVal(job.Slug),
		Type:       strVal(job.Type),
		Resolution: strVal(job.Resolution),
	}
	if meta.Type == "" {
		meta.Type = enums.MediaTypeVideo
	}
	if meta.MediaSlug != "" && meta.FileSlug != "" {
		return meta, nil
	}
	return resolveJobMetaFromDB(ctx, job, meta)
}

// resolveJobMetaFromDB — ทางสำรองสำหรับ doc เก่าที่ไม่มี slug ติดมา
func resolveJobMetaFromDB(ctx context.Context, job *models.PrewarmQueue, meta *JobMeta) (*JobMeta, error) {
	media, err := models.MediaModel.FindByID(ctx, job.MediaID)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			log.Printf("ℹ️ Media %s not found — nothing to prewarm", job.MediaID)
			return nil, nil
		}
		return nil, fmt.Errorf("load media: %w", err)
	}
	if media.DeletedAt != nil || media.FileID == nil {
		log.Printf("ℹ️ Media %s deleted/orphaned — nothing to prewarm", job.MediaID)
		return nil, nil
	}

	file, err := models.FileModel.FindByID(ctx, *media.FileID)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			log.Printf("ℹ️ File %s not found — nothing to prewarm", *media.FileID)
			return nil, nil
		}
		return nil, fmt.Errorf("load file: %w", err)
	}
	if file.Metadata != nil && (file.Metadata.DeletedAt != nil || file.Metadata.TrashedAt != nil) {
		log.Printf("ℹ️ File %s is trashed/deleted — nothing to prewarm", file.ID)
		return nil, nil
	}
	if file.Status != enums.FileStatusReady && file.Status != enums.FileStatusReadyOriginal {
		log.Printf("ℹ️ File %s not playable (status=%s) — skipped", file.ID, file.Status)
		return nil, nil
	}

	meta.MediaSlug = media.Slug
	meta.FileSlug = file.Slug
	meta.Type = media.Type
	if media.Resolution != nil {
		meta.Resolution = *media.Resolution
	}
	return meta, nil
}

func strVal(p *string) string {
	if p == nil {
		return ""
	}
	return *p
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
