package enums

// ─── Storage Types ───────────────────────────────────────────────────

const (
	StorageTypeLocal = "local"
	StorageTypeS3    = "s3"
)

// ─── Storage Statuses ────────────────────────────────────────────────

const (
	StorageStatusOnline      = "online"
	StorageStatusOffline     = "offline"
	StorageStatusError       = "error"
	StorageStatusMaintenance = "maintenance"
)

// ─── Storage Accepts ─────────────────────────────────────────────────

const (
	StorageAcceptUpload  = "upload"
	StorageAcceptTemp    = "temp"
	StorageAcceptStorage = "storage"
	StorageAcceptVideo   = "video"
	StorageAcceptImage   = "image"
	StorageAcceptOther   = "other"
)
