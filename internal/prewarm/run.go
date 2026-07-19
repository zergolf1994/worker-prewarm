package prewarm

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"worker-prewarm/internal/core/enums"
	"worker-prewarm/internal/core/utils"
	"worker-prewarm/internal/db/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// ─── Prewarm pipeline ────────────────────────────────────────
//
// collect: master playlist → child playlist ต่อ rendition → segment ทั้งหมด
//          + sprite.vtt/รูป (ถ้ามี) + master/vtt ของ cloned files
// warm:    HEAD ทุก URL ผ่านโดเมนสาธารณะ (CF) — MISS ครั้งนี้คือ HIT
//          ของผู้ชมคนแรก
//
// เสร็จ → ประทับ files.prewarmAt (enqueuer ใช้ตัดสิน re-prewarm)

// Run executes one prewarm job. Blocking; respects ctx cancellation.
func Run(ctx context.Context, job *models.VideoProcess) error {
	if job.FileID == nil {
		return fmt.Errorf("job has no fileId")
	}
	fileID := *job.FileID

	plog := utils.NewProcessLogger(strPtr(job.Slug))
	defer plog.Close()

	// ── Load file ─────────────────────────────────────────────
	file, err := models.FileModel.FindByID(ctx, fileID)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			// ไฟล์ถูกลบไปแล้ว — งานนี้ไม่มีอะไรให้ทำ จบแบบสำเร็จ
			log.Printf("ℹ️ File %s not found — nothing to prewarm", fileID)
			return nil
		}
		return fmt.Errorf("load file: %w", err)
	}
	if file.Metadata != nil && (file.Metadata.DeletedAt != nil || file.Metadata.TrashedAt != nil) {
		log.Printf("ℹ️ File %s is trashed/deleted — nothing to prewarm", fileID)
		return nil
	}
	if file.Status != enums.FileStatusReady && file.Status != enums.FileStatusReadyOriginal {
		return fmt.Errorf("file %s not playable (status=%s)", fileID, file.Status)
	}

	// ── Settings ──────────────────────────────────────────────
	domainPlaylist := normalizeDomain(getSettingString(ctx, enums.SettingDomainPlaylist))
	if domainPlaylist == "" {
		return fmt.Errorf("setting domain_playlist is not set")
	}
	referer := normalizeDomain(getSettingString(ctx, enums.SettingDomainPlayer))
	parallel := prewarmParallel(ctx)

	engine := NewEngine(parallel, referer)
	utils.LogMain("🔥 [%s] Prewarm start (domain=%s, parallel=%d)", file.Slug, domainPlaylist, parallel)

	// ── Step 1: collect URLs ──────────────────────────────────
	startStep(ctx, job.ID, "collect")

	masterURL := fmt.Sprintf("%s/%s/playlist.m3u8", domainPlaylist, file.Slug)
	urls, err := engine.CollectVideoURLs(ctx, masterURL)
	if err != nil {
		return err
	}

	urlSet := map[string]bool{}
	for _, u := range urls {
		urlSet[u] = true
	}

	// sprite.vtt + รูป (ยังไม่มี sprite ก็ข้ามเงียบๆ)
	for _, u := range engine.CollectVTTURLs(ctx, fmt.Sprintf("%s/%s/sprite/sprite.vtt", domainPlaylist, file.Slug)) {
		urlSet[u] = true
	}

	// cloned files — ใช้ medias ชุดเดียวกับต้นฉบับ (child/segment ซ้ำกัน)
	// ต่างแค่ master playlist กับ sprite.vtt ต่อ slug — warm เพิ่มเฉพาะสองตัวนี้
	cloneSlugs := collectCloneSlugs(ctx, fileID)
	for _, s := range cloneSlugs {
		urlSet[fmt.Sprintf("%s/%s/playlist.m3u8", domainPlaylist, s)] = true
		urlSet[fmt.Sprintf("%s/%s/sprite/sprite.vtt", domainPlaylist, s)] = true
	}

	allURLs := make([]string, 0, len(urlSet))
	for u := range urlSet {
		allURLs = append(allURLs, u)
	}
	completeStep(ctx, job.ID, "collect")
	utils.LogMain("📋 [%s] Collected %d URLs (%d clone slug(s))", file.Slug, len(allURLs), len(cloneSlugs))

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// ── Step 2: warm ──────────────────────────────────────────
	startStep(ctx, job.ID, "warm")
	start := time.Now()

	stats := engine.Warm(ctx, allURLs, warmProgress(job.ID))

	// shutdown/cancel กลางคัน — คืนงานเข้าคิว (สถิติไม่ครบ ไม่บันทึก)
	if ctx.Err() != nil {
		return ctx.Err()
	}

	completeStep(ctx, job.ID, "warm")

	// บันทึกสถิติลง job doc (โชว์ในหน้า history)
	models.VideoProcessModel.UpdateByID(ctx, job.ID, bson.M{"$set": bson.M{
		"prewarm": stats,
	}})

	utils.LogMain("✅ [%s] Warmed %d URLs (HIT:%d MISS:%d EXPIRED:%d FAILED:%d) in %s",
		file.Slug, stats.Total, stats.Hit, stats.Miss, stats.Expired, stats.Failed,
		time.Since(start).Round(time.Second))

	// ทั้งชุดล้มเหลว = ปลายทางมีปัญหาจริง (โดเมน/CF/origin) — ให้ retry
	if stats.Total > 0 && stats.Failed == stats.Total {
		return fmt.Errorf("all %d urls failed to warm", stats.Total)
	}

	// ประทับเวลาให้ enqueuer — ทำท้ายสุดเพื่อให้ failed job ไม่ถูกนับว่า warm แล้ว
	if err := markPrewarmed(ctx, fileID, stats); err != nil {
		return fmt.Errorf("mark prewarmed: %w", err)
	}

	return nil
}

func strPtr(s *string) string {
	if s == nil {
		return "unknown"
	}
	return *s
}
