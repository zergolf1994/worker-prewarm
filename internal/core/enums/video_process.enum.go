package enums

// ─── Process Types ───────────────────────────────────────────────────
// Must match WorkerType enum in vdohide-service (worker.enum.ts) —
// video_process.processType must equal worker.type for job dispatch.

const (
	ProcessTypeDownload    = "download"
	ProcessTypePrewarm     = "prewarm"
	ProcessTypeTranscode   = "transcode"
	ProcessTypeTransfer    = "transfer"
	ProcessTypeSpritesheet = "spritesheet"
)

// ─── Process Statuses ────────────────────────────────────────────────
// pending → processing → completed | failed | cancelled
// Must match VideoProcessStatus in vdohide-service (video-process.enum.ts).

const (
	ProcessStatusPending    = "pending"
	ProcessStatusProcessing = "processing"
	ProcessStatusCompleted  = "completed"
	ProcessStatusFailed     = "failed"
	ProcessStatusCancelled  = "cancelled"
)

// VideoProcessOpenStatuses = jobs still "open" (not finished). Must match
// VIDEO_PROCESS_OPEN_STATUSES on the Node side — used by its partial
// unique index {fileId, processType} and the enqueuer's dedupe filter.
var VideoProcessOpenStatuses = []string{
	ProcessStatusPending,
	ProcessStatusProcessing,
}

// ─── Step Statuses ───────────────────────────────────────────────────
// Per-step progress inside timeline (download/merge/probe/upload...).

const (
	StepStatusPending    = "pending"
	StepStatusProcessing = "processing"
	StepStatusCompleted  = "completed"
	StepStatusFailed     = "failed"
)
