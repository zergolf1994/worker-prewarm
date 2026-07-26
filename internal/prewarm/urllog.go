package prewarm

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ─── URL log (ไฟล์ต่อ media) ─────────────────────────────────
//
// หนึ่ง media = หนึ่งไฟล์ logs/{mediaSlug}.log — เขียนทับทุกครั้งที่ warm
// จบ จึงเห็นผลของ "รอบล่าสุด" ของ media นั้นเสมอ ไม่พอกขึ้นเรื่อยๆ
//
// เขียนครั้งเดียวตอนงานจบ (ไม่ใช่ราย URL) — ระหว่าง warm สะสมไว้ใน memory
// ก่อน เพื่อไม่ให้ 20 goroutine แย่งเขียนไฟล์กันทุกมิลลิวินาที
//
// รูปแบบ — หัวไฟล์สรุปผลรอบนี้ แล้วตามด้วยรายการ URL
// (url ไว้ท้ายสุดเพราะยาวไม่เท่ากัน คอลัมน์หน้าจะได้ตรงกัน):
//
//	date    : 2026-07-26T23:40:12+07:00
//	media   : Z-6wezQ_tcwTa (original)
//	file    : ygVrCU2_H-DuV
//	kind    : reprewarm @ fra
//	summary : Warmed 1790 URLs (HIT:1 MISS:1789 EXPIRED:0 FAILED:0) in 44.165s
//
//	2026-07-26T23:40:12+07:00 200 HIT   45ms https://vh002.xyz/Z-6wezQ/v-1.jpeg
//	2026-07-26T23:40:12+07:00 000 ERR 30.0s https://vh002.xyz/Z-6wezQ/v-2.jpeg timeout

// โหมดเก็บ log
const (
	URLLogOff   = "off"   // ไม่เก็บเลย
	URLLogError = "error" // หัวบล็อก + เฉพาะ URL ที่ล้มเหลว
	URLLogAll   = "all"   // หัวบล็อก + ทุก URL
)

var (
	urlLogMu   sync.RWMutex
	urlLogDir  string
	urlLogMode = URLLogOff
)

// InitURLLog เตรียมโฟลเดอร์เก็บ log — mode "off" = ไม่ทำอะไรเลย
func InitURLLog(dir, mode string) error {
	mode = strings.ToLower(strings.TrimSpace(mode))
	switch mode {
	case URLLogError, URLLogAll:
	default:
		urlLogMu.Lock()
		urlLogMode = URLLogOff
		urlLogMu.Unlock()
		return nil
	}

	if dir == "" {
		dir = "logs"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create log dir %s: %w", dir, err)
	}

	urlLogMu.Lock()
	urlLogDir = dir
	urlLogMode = mode
	urlLogMu.Unlock()
	return nil
}

// urlLogEnabled บอกว่าต้องสะสม outcome ไว้เขียนไหม — เช็คก่อนจองหน่วยความจำ
func urlLogEnabled() bool {
	urlLogMu.RLock()
	defer urlLogMu.RUnlock()
	return urlLogMode != URLLogOff && urlLogDir != ""
}

// isFailure — ผลที่ถือว่าไม่ปกติ (ใช้ตอน mode = error)
func isFailure(o URLOutcome) bool {
	return o.Err != nil || (o.Status != 200 && o.Status != 206)
}

// JobLogInfo คือหัวบล็อกของ media หนึ่งตัว
type JobLogInfo struct {
	MediaSlug  string
	FileSlug   string
	Resolution string
	Kind       string
	Pop        string
	Took       time.Duration
}

// WriteJobLog เขียนไฟล์ของ media นี้ทับของเดิมในครั้งเดียว
func WriteJobLog(info JobLogInfo, outcomes []URLOutcome, stats WarmStats) {
	urlLogMu.RLock()
	dir, mode := urlLogDir, urlLogMode
	urlLogMu.RUnlock()

	if dir == "" || mode == URLLogOff {
		return
	}

	name := safeFileName(info.MediaSlug)
	if name == "" {
		return // ไม่มี slug ก็ไม่รู้จะตั้งชื่อไฟล์ว่าอะไร
	}

	now := time.Now().Format(time.RFC3339)

	var sb strings.Builder
	sb.Grow(len(outcomes)*96 + 320)

	// หัวไฟล์ — สรุปผลรอบนี้แบบอ่านรวดเดียวจบ (ถ้อยคำเดียวกับที่ขึ้น stdout)
	fmt.Fprintf(&sb, "date    : %s\n", now)
	fmt.Fprintf(&sb, "media   : %s (%s)\n", info.MediaSlug, info.Resolution)
	fmt.Fprintf(&sb, "file    : %s\n", info.FileSlug)
	fmt.Fprintf(&sb, "kind    : %s @ %s\n", info.Kind, info.Pop)
	fmt.Fprintf(&sb, "summary : Warmed %d URLs (HIT:%d MISS:%d EXPIRED:%d FAILED:%d) in %s\n",
		stats.Total, stats.Hit, stats.Miss, stats.Expired, stats.Failed,
		info.Took.Round(time.Millisecond))
	sb.WriteString("\n")

	for _, o := range outcomes {
		if mode == URLLogError && !isFailure(o) {
			continue
		}

		cache := o.Cache
		if cache == "" {
			cache = "-"
		}
		errText := ""
		if o.Err != nil {
			cache = "ERR"
			errText = " " + sanitizeLogValue(o.Err.Error())
		}

		fmt.Fprintf(&sb, "%s %03d %-11s %8s %s%s\n",
			now, o.Status, cache,
			o.Duration.Round(time.Millisecond).String(),
			o.URL, errText)
	}

	path := filepath.Join(dir, name+".log")
	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		// เขียน log ไม่ได้ ไม่ควรทำให้งาน warm ล้ม — แค่บอกไว้บน stdout
		logURLWriteError(path, err)
	}
}

// logURLWriteError เตือนแบบไม่ให้ท่วมจอ — ครั้งแรกครั้งเดียวต่อการรัน
var urlLogErrOnce sync.Once

func logURLWriteError(path string, err error) {
	urlLogErrOnce.Do(func() {
		fmt.Fprintf(os.Stderr, "⚠️ write url log %s: %v (จะไม่เตือนซ้ำ)\n", path, err)
	})
}

// safeFileName กันชื่อ slug แปลกๆ ทำ path traversal / ตัวอักษรที่ FS ไม่รับ
func safeFileName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := b.String()
	if len(out) > 120 {
		out = out[:120]
	}
	return out
}

// sanitizeLogValue ตัดขึ้นบรรทัดใหม่ออก กันข้อความ error ทำบรรทัดแตก
func sanitizeLogValue(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}
