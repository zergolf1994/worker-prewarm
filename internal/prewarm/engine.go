package prewarm

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ─── Warm engine ─────────────────────────────────────────────
//
// GET playlist → แตก URL ลูกทั้งหมด (child m3u8 / segment / รูป sprite)
// → HEAD ทุก URL พร้อมกันตาม parallel — CF cache ปลายทางถูก populate
// จากการยิงนี้ (MISS ครั้งแรก → HIT ครั้งถัดไป)

// WarmStats นับผลรวมของการ warm หนึ่งชุด URL
type WarmStats struct {
	Total   int64 `bson:"total" json:"total"`
	Hit     int64 `bson:"hit" json:"hit"`
	Miss    int64 `bson:"miss" json:"miss"`
	Expired int64 `bson:"expired" json:"expired"`
	Failed  int64 `bson:"failed" json:"failed"`
}

// Engine ยิง HTTP ไปที่โดเมนสาธารณะ (ผ่าน CF) ด้วย connection pool เดียว
type Engine struct {
	client   *http.Client
	referer  string
	parallel int
}

// NewEngine สร้าง engine — referer ว่างได้ (ไม่ใส่ header)
func NewEngine(parallel int, referer string) *Engine {
	if parallel <= 0 {
		parallel = 20
	}
	return &Engine{
		referer:  referer,
		parallel: parallel,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        parallel * 2,
				MaxIdleConnsPerHost: parallel,
				IdleConnTimeout:     30 * time.Second,
			},
			// HEAD ไม่ต้องตาม redirect — สถานะ 30x ก็คือคำตอบจาก edge แล้ว
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

const userAgent = "Mozilla/5.0 (compatible; VdoHide-Prewarm/1.0)"

// fetchContent GET แล้วคืน body — ใช้อ่าน playlist/vtt
func (e *Engine) fetchContent(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", userAgent)
	if e.referer != "" {
		req.Header.Set("Referer", e.referer)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20)) // playlist ไม่ควรเกิน 10MB
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// CollectVideoURLs ไล่จาก master playlist → child playlist → segment
// คืนทุก URL ที่ต้อง warm (รวม master เอง)
func (e *Engine) CollectVideoURLs(ctx context.Context, masterURL string) ([]string, error) {
	urlSet := map[string]bool{masterURL: true}

	masterContent, err := e.fetchContent(ctx, masterURL)
	if err != nil {
		return nil, fmt.Errorf("fetch master playlist: %w", err)
	}
	baseURL := masterURL[:strings.LastIndex(masterURL, "/")+1]

	var childPlaylists []string
	for _, line := range strings.Split(masterContent, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasSuffix(line, ".m3u8") || strings.Contains(line, ".m3u8?") {
			childPlaylists = append(childPlaylists, line)
		}
	}

	if len(childPlaylists) > 0 {
		// master แบบ multi-variant → ไล่ child ทีละตัว
		for _, child := range childPlaylists {
			childURL := buildURL(child, baseURL)
			urlSet[childURL] = true

			childContent, err := e.fetchContent(ctx, childURL)
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				continue // child เดียวพัง — ยัง warm ตัวอื่นต่อได้
			}
			childBase := childURL[:strings.LastIndex(childURL, "/")+1]
			for _, seg := range parseSegments(childContent) {
				urlSet[buildURL(seg, childBase)] = true
			}
		}
	} else {
		// playlist เดี่ยว (มี segment ตรงๆ)
		for _, seg := range parseSegments(masterContent) {
			urlSet[buildURL(seg, baseURL)] = true
		}
	}

	urls := make([]string, 0, len(urlSet))
	for u := range urlSet {
		urls = append(urls, u)
	}
	return urls, nil
}

// CollectVTTURLs อ่าน sprite.vtt แล้วคืน URL รูปทั้งหมด (รวมตัว vtt เอง)
// vtt ไม่มี (ยังไม่ทำ sprite) → คืน nil เฉยๆ ไม่ใช่ error
func (e *Engine) CollectVTTURLs(ctx context.Context, vttURL string) []string {
	content, err := e.fetchContent(ctx, vttURL)
	if err != nil {
		return nil
	}
	baseURL := vttURL[:strings.LastIndex(vttURL, "/")+1]

	urlSet := map[string]bool{vttURL: true}
	for _, img := range parseVTTImages(content) {
		urlSet[buildURL(img, baseURL)] = true
	}

	urls := make([]string, 0, len(urlSet))
	for u := range urlSet {
		urls = append(urls, u)
	}
	return urls
}

// Warm ยิง HEAD ทุก URL (parallel ตาม engine) — onProgress ถูกเรียกทุกครั้ง
// ที่ URL หนึ่งเสร็จ (nil ได้)
func (e *Engine) Warm(ctx context.Context, urls []string, onProgress func(done, total int64)) WarmStats {
	stats := WarmStats{Total: int64(len(urls))}
	var done int64

	var wg sync.WaitGroup
	sem := make(chan struct{}, e.parallel)

	for _, url := range urls {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(u string) {
			defer wg.Done()
			defer func() { <-sem }()

			e.headRequest(ctx, u, &stats)
			d := atomic.AddInt64(&done, 1)
			if onProgress != nil {
				onProgress(d, stats.Total)
			}
		}(url)
	}
	wg.Wait()
	return stats
}

// headRequest ยิง HEAD หนึ่ง URL แล้วอัพเดต stats ตาม CF-Cache-Status
func (e *Engine) headRequest(ctx context.Context, url string, stats *WarmStats) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		atomic.AddInt64(&stats.Failed, 1)
		return
	}
	req.Header.Set("User-Agent", userAgent)
	if e.referer != "" {
		req.Header.Set("Referer", e.referer)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		atomic.AddInt64(&stats.Failed, 1)
		return
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		atomic.AddInt64(&stats.Failed, 1)
		return
	}

	cacheStatus := resp.Header.Get("CF-Cache-Status")
	if cacheStatus == "" {
		cacheStatus = resp.Header.Get("X-Cache")
	}
	switch cacheStatus {
	case "HIT", "REVALIDATED":
		atomic.AddInt64(&stats.Hit, 1)
	case "EXPIRED":
		atomic.AddInt64(&stats.Expired, 1)
	default: // MISS / DYNAMIC / ไม่มี header — นับเป็น miss (= เพิ่ง warm)
		atomic.AddInt64(&stats.Miss, 1)
	}
}

// ─── Parsers ─────────────────────────────────────────────────

// parseSegments ดึงชื่อ segment จากเนื้อ m3u8 (.ts / .jpeg / URL เต็ม)
func parseSegments(content string) []string {
	var segments []string
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasSuffix(line, ".ts") || strings.HasSuffix(line, ".jpeg") ||
			strings.HasSuffix(line, ".mp4") || strings.HasSuffix(line, ".m4s") ||
			strings.HasPrefix(line, "http") || strings.HasPrefix(line, "//") {
			segments = append(segments, line)
		}
	}
	return segments
}

// parseVTTImages ดึงชื่อไฟล์รูป (unique) จากเนื้อ WebVTT sprite map
func parseVTTImages(content string) []string {
	var images []string
	seen := map[string]bool{}
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "WEBVTT") || strings.HasPrefix(line, "NOTE") ||
			strings.Contains(line, "-->") {
			continue
		}
		if strings.Contains(line, ".jpg") || strings.Contains(line, ".jpeg") || strings.Contains(line, ".png") {
			part := line
			if idx := strings.Index(line, "#"); idx >= 0 {
				part = line[:idx]
			}
			part = strings.TrimSpace(part)
			if part != "" && !seen[part] {
				seen[part] = true
				images = append(images, part)
			}
		}
	}
	return images
}

// buildURL ต่อ URL แบบ relative เข้ากับ base (รองรับ absolute / //host / /path)
func buildURL(segment, base string) string {
	if strings.HasPrefix(segment, "http") {
		return segment
	}
	if strings.HasPrefix(segment, "//") {
		return "https:" + segment
	}
	if strings.HasPrefix(segment, "/") {
		// path จาก root — ต้องเอาเฉพาะ scheme+host ของ base
		if idx := strings.Index(base[8:], "/"); idx > 0 {
			return base[:idx+8] + segment
		}
		return strings.TrimRight(base, "/") + segment
	}
	return base + segment
}
