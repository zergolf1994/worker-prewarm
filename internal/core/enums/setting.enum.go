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

	// domain_preview = โดเมน player หลักที่ Cloudflare referer allowlist อนุญาต
	// worker ต้องส่งค่านี้เป็น Referer ทุก request เพื่อผ่าน playlist/segment block rules
	SettingDomainPreview = "domain_preview"
)
