# worker-prewarm

CDN cache prewarmer worker for VdoHide — claims per-media jobs from the
dedicated **`prewarm_queue`** collection, walks the public HLS playlist of
that media, and fires concurrent `HEAD` requests through the CDN
(Cloudflare) so the first real viewer hits a warm edge cache.

```
enqueuer (vdohide-service)                 worker-prewarm (this repo)
┌──────────────────────────────┐           ┌─────────────────────────────┐
│ per pop with alive workers:  │  pending  │ Claim (atomic, own pop)     │
│  new: media ยังไม่มี          │ ────────▶ │ video → video.m3u8 +        │
│   prewarm.{pop} (non-fra     │           │   segments + master ของ     │
│   ต้องรอ fra เสร็จก่อน)       │           │   file + clones             │
│  reprewarm: prewarmAt เก่า    │           │ thumbnail → sprite.vtt +    │
│   กว่า reprewarmMinutes      │           │   sprite images             │
└──────────────────────────────┘           │ → medias.prewarm.{pop} แล้ว │
                                           │   ลบ doc ออกจากคิว          │
                                           └─────────────────────────────┘
```

## Design

- **คิวแยก** — งาน prewarm อยู่ใน `prewarm_queue` ไม่ปนกับ `video_process`
  ระหว่างทำงานไม่อัพเดต progress ใดๆ เสร็จแล้วบันทึกผลลง
  `medias.prewarm.{pop} = {data: {total,hit,miss,expired,failed}, prewarmAt}`
  (shape เดิมของระบบเก่า) แล้วลบ doc ทิ้ง — คิวเก็บเฉพาะงานค้างเสมอ
- **Multi-POP** — worker ประกาศ pop ของตัวเอง (`PREWARM_POP`) ผ่าน heartbeat
  และ claim เฉพาะงานของ pop ตัวเอง; enqueuer จัดคิวเฉพาะ pop ที่มี worker
  มีชีวิต งาน new ของ pop อื่นจะยังไม่ถูกจัดจนกว่า **fra** จะ warm media
  ชิ้นนั้นเสร็จก่อน
- **Storage binding** — worker ที่ตั้ง `STORAGE_ID` จะรับงาน **new** เฉพาะ
  media ของ storage ตัวเอง (enqueuer ประทับ `targetStorageId` เมื่อ storage
  นั้นมี worker ผูกอยู่) ส่วนงาน **reprewarm** ไม่ประทับ target — worker
  ไหนก็หยิบได้ ไม่ว่ามี storageId หรือไม่
- ล้มเหลว (playlist fetch ไม่ได้) ก็บันทึกผลเป็น failed ลง media เหมือน
  ระบบเก่า — ไปรอรอบ reprewarm ตามอายุ ไม่ค้างคิว

## What gets warmed per job

| Job type | URLs |
|---|---|
| video media | `/{mediaSlug}/video.m3u8` + ทุก segment + `/{fileSlug}/playlist.m3u8` + master ของ cloned files |
| thumbnail media | `/{fileSlug}/sprite/sprite.vtt` + `sprite-*.jpg` ทั้งหมด |

## Settings (MongoDB `settings` collection)

| Name | Shape | Description |
|---|---|---|
| `prewarm_config` | `{enabled, slotRate, reprewarmMinutes, parallel}` | `enabled` = kill switch (worker + enqueuer), `slotRate` = queue depth per worker, `reprewarmMinutes` = re-warm age (0 = off, default 1440), `parallel` = concurrent HEADs per job (default 20) |
| `domain_playlist` | string | public content-node domain — **required**, งานถูกคืนคิวถ้าไม่ตั้ง |
| `domain_player` | string | optional; ใส่เป็น `Referer` ทุก request |

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `mongodb://localhost:27017` | MongoDB connection string |
| `PREWARM_POP` | _(auto)_ | edge location ของเครื่องนี้ — ไม่ตั้ง = ตรวจเองจาก CF-Ray ตอน start (ยิง HEAD ไป domain_playlist/storage แล้วอ่าน colo) |
| `STORAGE_ID` | _(empty)_ | ผูกกับ storage — งาน new เฉพาะ media ของ storage นี้ |
| `WORKER_ID` | `prewarm_{hostname}@1` | unique worker id (`prewarm_` prefix required) |
| `LOG_PATH` | `logs/worker-prewarm.log` | rotating log file |

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/zergolf1994/worker-prewarm/main/install.sh | sudo -E bash -s -- \
    --database-url "mongodb+srv://user:pass@host/db"
```

### POP อื่น / ผูกกับ storage

```bash
... | sudo -E bash -s -- --database-url "..." --pop sin
... | sudo -E bash -s -- --database-url "..." --storage-id "storage-uuid"
```

### Update binary only (preserve `.env`)

```bash
curl -fsSL https://raw.githubusercontent.com/zergolf1994/worker-prewarm/main/install.sh | sudo bash -s --
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
