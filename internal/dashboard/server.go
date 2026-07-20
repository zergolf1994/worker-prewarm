package dashboard

import (
	_ "embed"
	"log"
	"net/http"
	"time"
)

//go:embed index.html
var indexHTML []byte

// Start เปิด dashboard realtime ที่ :port (blocking — เรียกใน goroutine)
// port ชน (เช่นรันหลาย instance บนเครื่องเดียว) → log เตือนแล้วจบเฉยๆ
// worker หลักทำงานต่อได้ปกติ
func Start(port string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	mux.HandleFunc("/events", GetHub().ServeEvents)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("📺 Dashboard: http://localhost:%s", port)
	if err := srv.ListenAndServe(); err != nil {
		log.Printf("⚠️ Dashboard disabled: %v", err)
	}
}
