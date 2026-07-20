package prewarm

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"worker-prewarm/internal/config"
	"worker-prewarm/internal/core/enums"
	"worker-prewarm/internal/db/models"
	"worker-prewarm/internal/queue"

	"go.mongodb.org/mongo-driver/mongo"
)

// ─── Prewarm pipeline (รายชิ้น media — แบบระบบเก่า) ─────────────
//
// คิวถือแค่ mediaId — ข้อมูลจริง (type/slug/fileId) ดึงจาก medias ตอน
// รับงาน ไม่เชื่อค่าที่ก็อปไว้ในคิว (media อาจถูกลบ/ย้ายระหว่างรอคิว)
//
//   video media     → master /{fileSlug}/playlist.m3u8 + child
//                     /{mediaSlug}/video.m3u8 + segment ทั้งหมด
//                     + master ของ cloned files (clone ใช้ media ร่วมกัน)
//   thumbnail media → /{fileSlug}/sprite/sprite.vtt + รูป sprite ทั้งหมด
//
// ระหว่างทำงานไม่อัพเดตอะไร — เสร็จแล้วบันทึกผลลง medias.prewarm.{pop}
// (แม้ fetch fail ก็บันทึก — เหมือนระบบเก่า — ไปรอ reprewarm ตามอายุ)
// แล้ว loop ลบ doc ออกจากคิว (สำเร็จหรือ fail ครบ retry ก็ลบ)

// Run executes one prewarm job. Blocking; respects ctx cancellation.
func Run(ctx context.Context, job *models.PrewarmQueue) error {
	pop := config.AppConfig.Pop

	// ── Settings ──────────────────────────────────────────────
	domainPlaylist := normalizeDomain(getSettingString(ctx, enums.SettingDomainPlaylist))
	if domainPlaylist == "" {
		// config ไม่พร้อม — ไม่ใช่ความผิดของงาน คืนคิวพร้อมหน่วงเวลา
		return fmt.Errorf("setting domain_playlist is not set: %w", queue.ErrJobRequeue)
	}
	referer := normalizeDomain(getSettingString(ctx, enums.SettingDomainPlayer))
	parallel := prewarmParallel(ctx)

	// ── Load media (source of truth) ──────────────────────────
	media, err := models.MediaModel.FindByID(ctx, job.MediaID)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			log.Printf("ℹ️ Media %s not found — nothing to prewarm", job.MediaID)
			return nil // ลบ doc ทิ้งเฉยๆ
		}
		return fmt.Errorf("load media: %w", err)
	}
	if media.DeletedAt != nil {
		log.Printf("ℹ️ Media %s is deleted — nothing to prewarm", job.MediaID)
		return nil
	}
	if media.FileID == nil {
		log.Printf("ℹ️ Media %s has no fileId — nothing to prewarm", job.MediaID)
		return nil
	}

	// ── Load parent file (ต้องเล่นได้จริงถึงมี playlist ให้ warm) ──
	file, err := models.FileModel.FindByID(ctx, *media.FileID)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			log.Printf("ℹ️ File %s not found — nothing to prewarm", *media.FileID)
			return nil
		}
		return fmt.Errorf("load file: %w", err)
	}
	if file.Metadata != nil && (file.Metadata.DeletedAt != nil || file.Metadata.TrashedAt != nil) {
		log.Printf("ℹ️ File %s is trashed/deleted — nothing to prewarm", file.ID)
		return nil
	}
	if file.Status != enums.FileStatusReady && file.Status != enums.FileStatusReadyOriginal {
		log.Printf("ℹ️ File %s not playable (status=%s) — skipped", file.ID, file.Status)
		return nil
	}

	engine := NewEngine(parallel, referer)
	start := time.Now()

	// ── Collect URLs (ตาม type ของ media จริง) ────────────────
	var urls []string
	label := media.Slug
	if media.Type == enums.MediaTypeThumbnail {
		// sprite map: vtt + รูปทุกใบที่ vtt อ้างถึง
		label = file.Slug + "/sprite"
		urls = engine.CollectVTTURLs(ctx, fmt.Sprintf("%s/%s/sprite/sprite.vtt", domainPlaylist, file.Slug))
		if len(urls) == 0 {
			// vtt หายทั้งที่ media ยังอยู่ — บันทึกเป็น failed (แบบเก่า)
			// แล้วไปรอ reprewarm ตามอายุ ไม่ retry รัวๆ
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("⚠️ [%s] sprite.vtt not reachable — recorded as failed", label)
			return recordPrewarm(ctx, media.ID, pop, WarmStats{Total: 1, Failed: 1})
		}
	} else {
		childURL := fmt.Sprintf("%s/%s/video.m3u8", domainPlaylist, media.Slug)
		collected, err := engine.CollectVideoURLs(ctx, childURL)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("⚠️ [%s] playlist not reachable (%v) — recorded as failed", label, err)
			return recordPrewarm(ctx, media.ID, pop, WarmStats{Total: 1, Failed: 1})
		}

		urlSet := map[string]bool{}
		for _, u := range collected {
			urlSet[u] = true
		}
		// master playlist ของไฟล์ + ของ cloned files (URL ระดับ slug ต่างกัน
		// แต่ชี้ media ชุดเดียวกัน — warm เพิ่มแค่ master ต่อ slug)
		urlSet[fmt.Sprintf("%s/%s/playlist.m3u8", domainPlaylist, file.Slug)] = true
		for _, s := range collectCloneSlugs(ctx, file.ID) {
			urlSet[fmt.Sprintf("%s/%s/playlist.m3u8", domainPlaylist, s)] = true
		}
		urls = make([]string, 0, len(urlSet))
		for u := range urlSet {
			urls = append(urls, u)
		}
	}

	// ── Warm ──────────────────────────────────────────────────
	stats := engine.Warm(ctx, urls, nil)

	// shutdown/cancel กลางคัน — คืนงานเข้าคิว (สถิติไม่ครบ ไม่บันทึก)
	if ctx.Err() != nil {
		return ctx.Err()
	}

	log.Printf("✅ [%s@%s] Warmed %d URLs (HIT:%d MISS:%d EXPIRED:%d FAILED:%d) in %s",
		label, pop, stats.Total, stats.Hit, stats.Miss, stats.Expired, stats.Failed,
		time.Since(start).Round(time.Millisecond))

	return recordPrewarm(ctx, media.ID, pop, stats)
}
