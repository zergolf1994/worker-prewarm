package enums

// ─── Setting Keys ────────────────────────────────────────────────────

const (
	// prewarm = shape เดียวกับระบบเก่า (server-prewarm):
	//   {enabled, enabled_old, prewarm_max_concurrent,
	//    prewarm_old_max_concurrent, prewarm_parallel,
	//    prewarm_old_parallel, reprewarm_age_minutes}
	// worker อ่าน enabled/enabled_old เป็น kill switch และ
	// prewarm_parallel / prewarm_old_parallel ตามชนิดงาน
	SettingPrewarm = "prewarm"

	// domain_playlist = โดเมนหน้า content-node ที่ผู้ชมใช้จริง — จุดที่ต้อง
	// warm (ไม่ตั้ง = ทำงาน prewarm ไม่ได้ → job fail รอ retry)
	SettingDomainPlaylist = "domain_playlist"

	// domain_player = ใส่เป็น Referer ตอนยิง warm (ถ้าตั้งไว้) — บาง CDN
	// rule เช็ค referer; ไม่ตั้งก็ยิงเปล่าได้
	SettingDomainPlayer = "domain_player"
)
