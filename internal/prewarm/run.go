package prewarm

import (
	"context"
	"fmt"
	"time"

	"log"
	"worker-prewarm/internal/config"
	"worker-prewarm/internal/core/enums"
	"worker-prewarm/internal/db/models"
	"worker-prewarm/internal/queue"
)

// ─── Prewarm pipeline (รายชิ้น media — แบบระบบเก่า) ─────────────
//
//   video media     → master /{fileSlug}/playlist.m3u8 + child
//                     /{mediaSlug}/video.m3u8 + segment ทั้งหมด
//                     + master ของ cloned files (clone ใช้ media ร่วมกัน)
//   thumbnail media → /{fileSlug}/sprite/sprite.vtt + รูป sprite ทั้งหมด
//
// ระหว่างทำงานไม่อัพเดตอะไร — เสร็จแล้วบันทึกผลลง medias.prewarm.{pop}
// (แม้ fetch fail ก็บันทึก — เหมือนระบบเก่า — ไปรอ reprewarm ตามอายุ)
// แล้ว loop ลบ doc ออกจากคิว

// Run executes one prewarm job. Blocking; respects ctx cancellation.
func Run(ctx context.Context, job *models.PrewarmQueue) error {
	fileSlug := strPtr(job.Slug)
	mediaSlug := strPtr(job.MediaSlug)
	pop := config.AppConfig.Pop

	// ── Settings ──────────────────────────────────────────────
	domainPlaylist := normalizeDomain(getSettingString(ctx, enums.SettingDomainPlaylist))
	if domainPlaylist == "" {
		// config ไม่พร้อม — ไม่ใช่ความผิดของงาน คืนคิวพร้อมหน่วงเวลา
		return fmt.Errorf("setting domain_playlist is not set: %w", queue.ErrJobRequeue)
	}
	referer := normalizeDomain(getSettingString(ctx, enums.SettingDomainPlayer))
	parallel := prewarmParallel(ctx)

	engine := NewEngine(parallel, referer)
	start := time.Now()

	// ── Collect URLs ──────────────────────────────────────────
	var urls []string
	label := mediaSlug
	if job.Type != nil && *job.Type == enums.MediaTypeThumbnail {
		label = fileSlug + "/sprite"
		urls = engine.CollectVTTURLs(ctx, fmt.Sprintf("%s/%s/sprite/sprite.vtt", domainPlaylist, fileSlug))
		if len(urls) == 0 {
			// vtt หายทั้งที่ media ยังอยู่ — บันทึกเป็น failed (แบบเก่า)
			// แล้วไปรอ reprewarm ตามอายุ ไม่ retry รัวๆ
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("⚠️ [%s] sprite.vtt not reachable — recorded as failed", label)
			return recordPrewarm(ctx, job.MediaID, pop, WarmStats{Total: 1, Failed: 1})
		}
	} else {
		childURL := fmt.Sprintf("%s/%s/video.m3u8", domainPlaylist, mediaSlug)
		collected, err := engine.CollectVideoURLs(ctx, childURL)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("⚠️ [%s] playlist not reachable (%v) — recorded as failed", label, err)
			return recordPrewarm(ctx, job.MediaID, pop, WarmStats{Total: 1, Failed: 1})
		}

		urlSet := map[string]bool{}
		for _, u := range collected {
			urlSet[u] = true
		}
		// master playlist ของไฟล์ + ของ cloned files (URL ระดับ slug ต่างกัน
		// แต่ชี้ media ชุดเดียวกัน — warm เพิ่มแค่ master ต่อ slug)
		urlSet[fmt.Sprintf("%s/%s/playlist.m3u8", domainPlaylist, fileSlug)] = true
		if job.FileID != nil {
			for _, s := range collectCloneSlugs(ctx, *job.FileID) {
				urlSet[fmt.Sprintf("%s/%s/playlist.m3u8", domainPlaylist, s)] = true
			}
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

	return recordPrewarm(ctx, job.MediaID, pop, stats)
}

func strPtr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
