package prewarm

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"path"

	"worker-prewarm/internal/config"
	"worker-prewarm/internal/core/enums"
	"worker-prewarm/internal/dashboard"
	"worker-prewarm/internal/db/models"
	"worker-prewarm/internal/queue"
)

// ─── Prewarm pipeline (รายชิ้น media) ──────────────────────────
//
// ข้อมูลงานอ่านจาก queue doc ตรงๆ (slug/mediaSlug/type/resolution ถูกก็อป
// มาให้ครบตอน enqueue และ enqueuer ตรวจ file gate มาแล้ว) — ไม่ยิง DB ซ้ำ
// doc เก่าที่ไม่มีฟิลด์พวกนี้ยังมี fallback ไปดึง DB ให้อัตโนมัติ
//
//   video media     → /{mediaSlug}/video.m3u8 + segment ของ rendition นั้น
//                     (1 job = 1 rendition — ไม่แตะ master playlist ระดับไฟล์)
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
	referer := normalizeDomain(getSettingString(ctx, enums.SettingDomainPreview))
	if referer == "" {
		return fmt.Errorf("setting domain_preview is not set: %w", queue.ErrJobRequeue)
	}
	kind := "new"
	if job.Kind != nil && *job.Kind != "" {
		kind = *job.Kind
	}
	parallel := prewarmParallel(ctx, kind)

	// ── ข้อมูลงานมาจาก queue doc ตรงๆ ─────────────────────────
	// enqueuer ตรวจ file gate (type video + status ready + ไม่ trash/ลบ) และ
	// ก็อป slug/type/resolution มาให้ครบตอน enqueue แล้ว — งานอยู่ในคิวแค่
	// ~1 นาที ไม่ต้องยิง DB ซ้ำเพื่อยืนยันอีก (150 job/นาที × 3 query =
	// ภาระที่ไม่ได้อะไรกลับมา) ถ้าไฟล์เพิ่งถูกลบจริง warm จะได้ 404 →
	// นับเป็น fail → เข้ากติกา retry ตามปกติ ไม่มีอะไรเสียหาย
	jobMeta, err := resolveJobMeta(ctx, job)
	if err != nil {
		return err
	}
	if jobMeta == nil {
		return nil // media/file หายไปแล้ว — ลบงานทิ้งเฉยๆ
	}
	mediaSlug, fileSlug, mediaType := jobMeta.MediaSlug, jobMeta.FileSlug, jobMeta.Type

	engine := NewEngine(parallel, referer)
	start := time.Now()

	// ── Collect URLs (ตาม type ของ media) ─────────────────────
	var urls []string
	label := mediaSlug
	if mediaType == enums.MediaTypeThumbnail {
		// sprite map: vtt + รูปทุกใบที่ vtt อ้างถึง
		label = fileSlug + "/sprite"
		urls = engine.CollectVTTURLs(ctx, fmt.Sprintf("%s/%s/sprite/sprite.vtt", domainPlaylist, fileSlug))
		if len(urls) == 0 {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// vtt ไม่ตอบ = fail 100% — บันทึกผลแล้วจบงาน (ไม่ retry)
			log.Printf("⚠️ [%s@%s] sprite.vtt not reachable — recorded as failed", label, pop)
			return recordPrewarm(ctx, job.MediaID, pop, WarmStats{Total: 1, Failed: 1})
		}
	} else {
		childURL := fmt.Sprintf("%s/%s/video.m3u8", domainPlaylist, mediaSlug)
		collected, err := engine.CollectVideoURLs(ctx, childURL)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// playlist ไม่ตอบ = fail 100% — บันทึกผลแล้วจบงาน (ไม่ retry)
			log.Printf("⚠️ [%s@%s] playlist not reachable — recorded as failed: %v", label, pop, err)
			return recordPrewarm(ctx, job.MediaID, pop, WarmStats{Total: 1, Failed: 1})
		}

		// งานคือ media ตัวนี้ตัวเดียว (1 job = 1 rendition) — ไม่แตะ master
		// playlist ระดับไฟล์ เพราะมันเป็นของรวมทุก rendition ถ้า warm ตรงนี้
		// ไฟล์ที่มีหลาย rendition จะถูก warm ซ้ำเท่าจำนวน rendition
		urlSet := map[string]bool{}
		for _, u := range collected {
			urlSet[u] = true
		}
		urls = make([]string, 0, len(urlSet))
		for u := range urlSet {
			urls = append(urls, u)
		}
	}

	// ── Warm (สตรีมผลราย URL ให้ dashboard แบบระบบเก่า) ────────
	hub := dashboard.GetHub()
	resLabel := "sprite"
	if mediaType != enums.MediaTypeThumbnail && jobMeta.Resolution != "" {
		resLabel = jobMeta.Resolution
	}
	kindLabel := kind
	hub.JobStarted(&dashboard.JobInfo{
		ID: job.ID, MediaSlug: mediaSlug, FileSlug: fileSlug,
		Resolution: resLabel, Kind: kindLabel, Pop: pop,
		Total: int64(len(urls)),
	})

	// สะสมผลราย URL ไว้เขียน log ทีเดียวตอนจบ (เปิดใช้เมื่อ URL log เปิดอยู่)
	var (
		outMu    sync.Mutex
		outcomes []URLOutcome
	)
	collect := urlLogEnabled()
	if collect {
		outcomes = make([]URLOutcome, 0, len(urls))
	}

	stats := engine.Warm(ctx, urls, func(o URLOutcome, done, total int64) {
		if collect {
			outMu.Lock()
			outcomes = append(outcomes, o)
			outMu.Unlock()
		}

		hub.JobProgress(job.ID, done, total)
		errStr := ""
		if o.Err != nil {
			errStr = o.Err.Error()
		}
		hub.Broadcast("url_result", dashboard.URLResult{
			JobID: job.ID, MediaSlug: mediaSlug, FileSlug: fileSlug,
			Resolution: resLabel, URL: path.Base(o.URL),
			Status: o.Status, Cache: o.Cache, Pop: pop,
			Duration: o.Duration.Round(time.Millisecond).String(),
			Error:    errStr, Progress: done, Total: total,
		})
	})
	defer hub.JobDone(job.ID, stats)

	// shutdown/cancel กลางคัน — คืนงานเข้าคิว (สถิติไม่ครบ ไม่บันทึก)
	if ctx.Err() != nil {
		return ctx.Err()
	}

	took := time.Since(start)
	log.Printf("✅ [%s@%s] Warmed %d URLs (HIT:%d MISS:%d EXPIRED:%d FAILED:%d) in %s",
		label, pop, stats.Total, stats.Hit, stats.Miss, stats.Expired, stats.Failed,
		took.Round(time.Millisecond))

	// เขียนรายการ URL ของ media นี้ลงไฟล์ — ทำหลัง warm จบ ก่อนตัดสินผล
	// เพื่อให้งานที่ fail เกินเกณฑ์ก็ยังมีรายการไว้ไล่ดูว่าพังตรงไหน
	if collect {
		WriteJobLog(JobLogInfo{
			MediaSlug: mediaSlug, FileSlug: fileSlug, Resolution: resLabel,
			Kind: kindLabel, Pop: pop, Took: took,
		}, outcomes, stats)
	}

	// บันทึกผลเสมอ ไม่ว่าจะ fail กี่ % — ไม่มี retry ในคิวอีกแล้ว
	//
	// เหตุผล: งานที่คา nextRetryAt กินโควตาต่อ storage ของ enqueuer ทั้งที่
	// ยังไม่ได้ใช้แบนด์วิดท์อะไรเลย และถ้า worker ของ targetStorageId ดับ
	// งานจะไม่มีใคร claim → retryCount ไม่เพิ่ม → ค้างกินโควตาถาวร
	//
	// พังแล้วบันทึกไปเลยแทน: prewarmAt ถูกตั้งเป็นตอนนี้ media จึงไปเข้าคิว
	// ใหม่เองผ่านช่อง reprewarm เมื่อครบ reprewarm_age_minutes
	if stats.Failed > 0 {
		failPct := float64(stats.Failed) / float64(stats.Total) * 100
		log.Printf("⚠️ [%s@%s] failed %d/%d urls (%.0f%%) — recorded, will retry via reprewarm",
			label, pop, stats.Failed, stats.Total, failPct)
	}

	return recordPrewarm(ctx, job.MediaID, pop, stats)
}
