# worker-prewarm

CDN cache prewarmer worker for VdoHide — claims `prewarm` jobs from the
`video_process` queue, walks the public HLS playlists of a file, and fires
concurrent `HEAD` requests through the CDN (Cloudflare) so the first real
viewer hits a warm edge cache.

```
enqueuer (vdohide-service)                 worker-prewarm (this repo)
┌──────────────────────────┐               ┌─────────────────────────────┐
│ transfer completed →     │   pending     │ Claim (atomic)              │
│   enqueue prewarm (new)  │ ────────────▶ │ collect: master playlist →  │
│ rolling cursor →         │               │   child m3u8 → segments +   │
│   re-prewarm old files   │               │   sprite.vtt/images + clones│
└──────────────────────────┘               │ warm: HEAD ทุก URL (CF)     │
                                           │ → files.prewarmAt + stats   │
                                           └─────────────────────────────┘
```

## What gets warmed

For file slug `abc` on `domain_playlist`:

| URL | Source |
|---|---|
| `/{slug}/playlist.m3u8` | master playlist (content-node) |
| `/{mediaSlug}/video.m3u8` | child playlist per rendition |
| segment URLs inside each child | storage `publicUrl` domains |
| `/{slug}/sprite/sprite.vtt` + `sprite-*.jpg` | thumbnail sprites (skipped if none) |
| master + sprite.vtt of cloned files | clones share media, only the slug-level URLs differ |

Cache status is read from `CF-Cache-Status` and recorded per job as
`{total, hit, miss, expired, failed}` — visible in the admin queue history.
On success the worker stamps `files.prewarmAt`, which the enqueuer uses to
decide re-prewarm.

## Queue behavior

- Pool worker — no storage binding; any prewarm worker claims any job.
- Claim is atomic (`FindOneAndUpdate` pending → processing), sorted by
  `priority: -1, createdAt: 1` (new files before re-prewarm).
- Retry with backoff (1m/2m, max 3) — a job only fails outright when the
  master playlist can't be fetched or **every** URL fails.
- Admin cancel is detected mid-run (poll every 5s) and aborts all HTTP I/O.
- Graceful shutdown (SIGTERM) releases the job back to the queue.

## Settings (MongoDB `settings` collection)

| Name | Shape | Description |
|---|---|---|
| `prewarm_config` | `{enabled, slotRate, reprewarmMinutes, parallel}` | `enabled` = kill switch (worker + enqueuer), `slotRate` = queue depth per worker, `reprewarmMinutes` = re-warm age (0 = off, default 1440), `parallel` = concurrent HEADs per job (default 20) |
| `domain_playlist` | string | public content-node domain — **required**, jobs fail without it |
| `domain_player` | string | optional; sent as `Referer` on every request |

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `mongodb://localhost:27017` | MongoDB connection string |
| `WORKER_ID` | `prewarm_{hostname}@1` | unique worker id (`prewarm_` prefix required) |
| `LOG_PATH` | `logs/worker-prewarm.log` | rotating log file |

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/zergolf1994/worker-prewarm/main/install.sh | sudo -E bash -s -- \
    --database-url "mongodb+srv://user:pass@host/db"
```

### Update binary only (preserve `.env`)

```bash
curl -fsSL https://raw.githubusercontent.com/zergolf1994/worker-prewarm/main/install.sh | sudo bash -s --
```

### Multiple workers on one machine

```bash
... | sudo -E bash -s -- --database-url "..." --count 2
```

### Uninstall

```bash
curl -fsSL https://raw.githubusercontent.com/zergolf1994/worker-prewarm/main/install.sh | sudo bash -s -- --uninstall
```

## Development

```bash
go build ./...          # build all packages
build.bat               # Windows binary → .build/windows.exe
```

Releases are built by GitHub Actions on `v*` tags (linux amd64 + arm64).
