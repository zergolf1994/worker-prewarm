package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
)

// ─── Realtime hub (SSE) ──────────────────────────────────────
//
// แบบเดียวกับ ws hub ของ server-prewarm เดิม แต่ใช้ Server-Sent Events —
// broadcast ทางเดียวเหมือนกัน ไม่ต้องพ่วง websocket dependency
// event: job_start / url_result / job_complete / snapshot (ตอน connect)

// Message is the envelope sent to clients.
type Message struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

// URLResult represents a single URL prewarm result (shape เดิมของระบบเก่า
// + jobId ให้หน้าเว็บผูกกับ card ของงาน).
type URLResult struct {
	JobID      string `json:"jobId"`
	MediaSlug  string `json:"mediaSlug"`
	FileSlug   string `json:"fileSlug"`
	Resolution string `json:"resolution"`
	URL        string `json:"url"`
	Status     int    `json:"status"`
	Cache      string `json:"cache"`
	Pop        string `json:"pop"`
	Duration   string `json:"duration"`
	Error      string `json:"error,omitempty"`
	Progress   int64  `json:"progress"`
	Total      int64  `json:"total"`
}

// JobInfo คืองานที่กำลัง warm อยู่ — ใช้ทั้ง job_start และ snapshot
type JobInfo struct {
	ID         string `json:"id"`
	MediaSlug  string `json:"mediaSlug"`
	FileSlug   string `json:"fileSlug"`
	Resolution string `json:"resolution"` // "sprite" สำหรับ thumbnail
	Kind       string `json:"kind"`
	Pop        string `json:"pop"`
	Progress   int64  `json:"progress"`
	Total      int64  `json:"total"`
}

// Hub manages SSE clients and the active-jobs snapshot.
type Hub struct {
	mu      sync.RWMutex
	clients map[chan []byte]bool
	active  map[string]*JobInfo // jobID → งานที่กำลังรัน (สำหรับ snapshot)

	// clientCount — อ่านแบบ atomic ให้ Broadcast เช็คได้โดยไม่ต้องจับ lock
	// (ถูกเรียกหลักร้อยครั้ง/วินาที ระหว่างที่ goroutine warm แย่ง mu กันอยู่)
	clientCount int64
}

var instance *Hub
var once sync.Once

func GetHub() *Hub {
	once.Do(func() {
		instance = &Hub{
			clients: make(map[chan []byte]bool),
			active:  make(map[string]*JobInfo),
		}
	})
	return instance
}

// ServeEvents handles GET /events (SSE stream).
func (h *Hub) ServeEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ch := make(chan []byte, 256)
	h.mu.Lock()
	h.clients[ch] = true
	atomic.AddInt64(&h.clientCount, 1)
	// ต่อเข้ามากลางงาน → ส่ง snapshot งานที่กำลังรันให้ก่อน
	snap := make([]*JobInfo, 0, len(h.active))
	for _, j := range h.active {
		snap = append(snap, j)
	}
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		delete(h.clients, ch)
		atomic.AddInt64(&h.clientCount, -1)
		h.mu.Unlock()
	}()

	writeEvent(w, Message{Type: "snapshot", Data: snap})
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

func writeEvent(w http.ResponseWriter, m Message) {
	b, err := json.Marshal(m)
	if err != nil {
		return
	}
	fmt.Fprintf(w, "data: %s\n\n", b)
}

// Broadcast sends a message to all connected clients (drop when buffer full).
// ไม่มีคนดู dashboard = ไม่ต้อง marshal — ปกติไม่มีใครเปิดค้างไว้ แต่ถูก
// เรียกทุก URL ที่ warm (หลักร้อยครั้งต่อวินาที) ถ้า marshal ทิ้งทุกครั้ง
// คือเผา CPU ให้ข้อความที่ไม่มีใครรับ
func (h *Hub) Broadcast(msgType string, data interface{}) {
	if atomic.LoadInt64(&h.clientCount) == 0 {
		return
	}

	b, err := json.Marshal(Message{Type: msgType, Data: data})
	if err != nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.clients {
		select {
		case ch <- b:
		default:
		}
	}
}

// ─── Active-job registry (สำหรับ snapshot ตอน client เพิ่งต่อ) ───

func (h *Hub) JobStarted(job *JobInfo) {
	h.mu.Lock()
	h.active[job.ID] = job
	h.mu.Unlock()
	h.Broadcast("job_start", job)
}

// JobProgress อัปเดตความคืบหน้าของงาน — ถูกเรียกจาก goroutine ที่ warm
// ทุกตัว ทุก URL (หลักร้อยครั้ง/วินาที) จึงใช้ read lock + atomic store
// แทน write lock เดิม ที่บังคับให้ goroutine ทั้งหมดต่อคิวกันทีละตัว
// (ฝั่งอ่าน snapshot จับ write lock อยู่แล้ว จึงไม่ชนกับการเขียนตรงนี้)
func (h *Hub) JobProgress(jobID string, progress, total int64) {
	h.mu.RLock()
	j, ok := h.active[jobID]
	h.mu.RUnlock()
	if !ok {
		return
	}
	atomic.StoreInt64(&j.Progress, progress)
	atomic.StoreInt64(&j.Total, total)
}

func (h *Hub) JobDone(jobID string, stats interface{}) {
	h.mu.Lock()
	j := h.active[jobID]
	delete(h.active, jobID)
	h.mu.Unlock()
	h.Broadcast("job_complete", map[string]interface{}{"job": j, "stats": stats})
}
