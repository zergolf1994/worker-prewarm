package prewarm

import (
	"context"
	"log"
	"net/http"
	"strings"
	"time"

	"worker-prewarm/internal/core/enums"
	"worker-prewarm/internal/db/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ─── POP auto-detect ─────────────────────────────────────────
//
// ยิง HEAD ไปโดเมนที่ผ่าน Cloudflare แล้วอ่าน colo code จาก header
// CF-Ray ("8cba…-SIN" → "sin") — edge ที่ตอบคือ edge ที่ใกล้เครื่องนี้
// = pop ที่การ warm ของเครื่องนี้จะไปอุ่นจริง
//
// ลำดับ URL ที่ลอง: domain_playlist (setting) → publicUrl ของ storage
// ที่ online — ตรวจไม่ได้เลย → fallback "fra"

const fallbackPop = "fra"

// DetectPop คืน pop ของเครื่องนี้ (lowercase) — เรียกครั้งเดียวตอน start
func DetectPop(ctx context.Context) string {
	urls := detectURLs(ctx)
	if len(urls) == 0 {
		log.Printf("⚠️ POP auto-detect: no CF-proxied URLs found — falling back to %q", fallbackPop)
		return fallbackPop
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        1,
			MaxIdleConnsPerHost: 1,
			DisableKeepAlives:   true, // บังคับ connection ใหม่ทุกครั้ง — อาจโดนคนละ edge
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// ยิงหลายรอบแล้วเอา pop ที่เจอบ่อยสุด — anycast อาจสลับ edge ได้
	const roundsPerURL = 2
	popCount := map[string]int{}
	for _, url := range urls {
		for i := 0; i < roundsPerURL; i++ {
			if ctx.Err() != nil {
				return fallbackPop
			}
			if pop := detectOnce(ctx, client, url); pop != "" {
				popCount[pop]++
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	best, bestCount := "", 0
	for pop, count := range popCount {
		if count > bestCount {
			best, bestCount = pop, count
		}
	}
	if best == "" {
		log.Printf("⚠️ POP auto-detect: no CF-Ray header seen — falling back to %q", fallbackPop)
		return fallbackPop
	}
	log.Printf("🌍 POP auto-detected: %s (%d/%d hits)", best, bestCount, len(urls)*roundsPerURL)
	return best
}

// detectURLs รวม URL ที่น่าจะผ่าน CF: domain_playlist ก่อน แล้วตามด้วย
// publicUrl ของ storage ที่เปิดใช้งานอยู่
func detectURLs(ctx context.Context) []string {
	urls := []string{}

	if d := normalizeDomain(getSettingString(ctx, enums.SettingDomainPlaylist)); d != "" {
		urls = append(urls, d+"/")
	}

	cursor, err := models.StorageModel.FindRaw(ctx, bson.M{
		"enable": true,
		"status": enums.StorageStatusOnline,
	}, options.Find().SetProjection(bson.M{"publicUrl": 1}))
	if err != nil {
		return urls
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var s models.Storage
		if err := cursor.Decode(&s); err != nil || s.PublicURL == nil {
			continue
		}
		// publicUrl เก็บได้หลายโดเมนคั่น comma
		for _, d := range strings.Split(*s.PublicURL, ",") {
			if d = normalizeDomain(d); d != "" {
				urls = append(urls, d+"/")
			}
		}
	}
	return urls
}

// detectOnce ยิง HEAD หนึ่งครั้งแล้วอ่าน colo จาก CF-Ray
func detectOnce(ctx context.Context, client *http.Client, url string) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	resp.Body.Close()

	cfRay := resp.Header.Get("CF-Ray")
	if cfRay == "" {
		return ""
	}
	parts := strings.Split(cfRay, "-")
	if len(parts) > 1 {
		return strings.ToLower(parts[len(parts)-1])
	}
	return ""
}
