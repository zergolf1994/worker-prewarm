package enums

// ─── Worker Types ────────────────────────────────────────────────────
// Same values as ProcessType* — a worker only claims jobs whose
// processType matches its type. Must match worker.enum.ts (Node).

const (
	WorkerTypeDownload    = ProcessTypeDownload
	WorkerTypePrewarm     = ProcessTypePrewarm
	WorkerTypeTranscode   = ProcessTypeTranscode
	WorkerTypeTransfer    = ProcessTypeTransfer
	WorkerTypeSpritesheet = ProcessTypeSpritesheet
)

// ─── Worker Statuses ─────────────────────────────────────────────────
// Reported via heartbeat. The enqueuer decides capacity from
// enable + fresh heartbeatAt — status is informational (admin display).
//   idle = free, busy = working, paused = disk full (enable=false too),
//   offline = graceful shutdown / stale heartbeat

const (
	WorkerStatusIdle    = "idle"
	WorkerStatusBusy    = "busy"
	WorkerStatusPaused  = "paused"
	WorkerStatusOffline = "offline"
)
