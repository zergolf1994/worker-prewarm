package enums

// ─── Setting Keys ────────────────────────────────────────────────────

const (
	// prewarm_config = {enabled, slotRate, reprewarmMinutes, parallel} —
	// shared with the vdohide-service enqueuer; worker reads .enabled as a
	// kill switch and .parallel as concurrent HEAD connections per job
	SettingPrewarmConfig = "prewarm_config"

	// domain_playlist = โดเมนหน้า content-node ที่ผู้ชมใช้จริง — จุดที่ต้อง
	// warm (ไม่ตั้ง = ทำงาน prewarm ไม่ได้ → job fail รอ retry)
	SettingDomainPlaylist = "domain_playlist"

	// domain_player = ใส่เป็น Referer ตอนยิง warm (ถ้าตั้งไว้) — บาง CDN
	// rule เช็ค referer; ไม่ตั้งก็ยิงเปล่าได้
	SettingDomainPlayer = "domain_player"
)
