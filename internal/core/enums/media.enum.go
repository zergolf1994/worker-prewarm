package enums

// ─── Media Types ─────────────────────────────────────────────────────
// Must match MediaType in vdohide-service (file.enum.ts).

const (
	MediaTypeVideo     = "video"
	MediaTypeAudio     = "audio"
	MediaTypeSubtitle  = "subtitle"
	MediaTypeThumbnail = "thumbnail"
	MediaTypeImage     = "image"
	MediaTypeDocument  = "document"
	MediaTypeOther     = "other"
)

// ─── Ingest Source Types ─────────────────────────────────────────────
// "processed" = created by the download worker (original ready for HLS).

const (
	IngestSourceTypeUpload    = "upload"
	IngestSourceTypeRemote    = "remote"
	IngestSourceTypeGDrive    = "gdrive"
	IngestSourceTypeS3Import  = "s3_import"
	IngestSourceTypeProcessed = "processed"
)

// ─── Resolution ──────────────────────────────────────────────────────

const (
	ResolutionOriginal = "original"
	Resolution1080     = "1080"
	Resolution720      = "720"
	Resolution480      = "480"
	Resolution360      = "360"
)

// ─── Asset file names on S3 temp / local storage ─────────────────────

const (
	FileNameOriginal = "file_original.mp4"
	FileName1080     = "file_1080.mp4"
	FileName720      = "file_720.mp4"
	FileName480      = "file_480.mp4"
	FileName360      = "file_360.mp4"
	SpriteZipName    = "sprite.zip"
	SpriteVTTName    = "sprite.vtt"
)

var ResolutionToFileName = map[string]string{
	ResolutionOriginal: FileNameOriginal,
	Resolution1080:     FileName1080,
	Resolution720:      FileName720,
	Resolution480:      FileName480,
	Resolution360:      FileName360,
}

// AllResolutions — install order: original first, then descending tiers.
var AllResolutions = []string{
	ResolutionOriginal,
	Resolution1080,
	Resolution720,
	Resolution480,
	Resolution360,
}

// TranscodedResolutions — resolutions produced by the HLS/transcode service.
var TranscodedResolutions = []string{
	Resolution1080,
	Resolution720,
	Resolution480,
	Resolution360,
}
