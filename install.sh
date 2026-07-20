#!/bin/bash

# Worker Prewarm Installation Script
# Usage: curl -fsSL https://raw.githubusercontent.com/zergolf1994/worker-prewarm/main/install.sh | sudo -E bash -s -- [OPTIONS]

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Defaults
WORKER_COUNT=1
UNINSTALL=false
DATABASE_URL=""
PREWARM_POP="fra"
STORAGE_ID=""

APP_NAME="worker-prewarm"
APP_DIR="/opt/$APP_NAME"
SERVICE_NAME="worker-prewarm"
GITHUB_REPO="zergolf1994/worker-prewarm"
RELEASES_URL="https://github.com/$GITHUB_REPO/releases/latest/download"

print_status()  { echo -e "${GREEN}[INFO]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --uninstall)         UNINSTALL=true; shift ;;
        --count|-w|-n)       WORKER_COUNT="$2"; shift 2 ;;
        --database-url)      DATABASE_URL="$2"; shift 2 ;;
        --mongodb-uri)       DATABASE_URL="$2"; shift 2 ;; # alias เดิม
        --pop)               PREWARM_POP="$2"; shift 2 ;;
        --storage-id)        STORAGE_ID="$2"; shift 2 ;;
        -h|--help)
            echo "Worker Prewarm Installer"
            echo ""
            echo "Usage: curl -fsSL https://raw.githubusercontent.com/$GITHUB_REPO/main/install.sh | sudo -E bash -s -- [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --uninstall          Uninstall completely"
            echo "  --count NUM          Number of worker instances (default: 1)"
            echo "  -w, -n NUM           Alias for --count"
            echo "  --database-url URI   MongoDB connection string (DATABASE_URL)"
            echo "  --pop POP            Edge location ของเครื่องนี้ (default: fra)"
            echo "  --storage-id ID      ผูกกับ storage — งานใหม่ warm เฉพาะ media ของ storage นี้"
            echo "                       (งาน reprewarm หยิบได้เสมอ; ไม่ใส่ = pool)"
            echo "  -h, --help           Show this help"
            echo ""
            echo "Examples:"
            echo "  # Install with 1 worker"
            echo "  curl -fsSL https://raw.githubusercontent.com/$GITHUB_REPO/main/install.sh | sudo -E bash -s -- \\"
            echo "      --database-url \"mongodb+srv://user:pass@host/db\""
            echo ""
            echo "  # Uninstall"
            echo "  curl -fsSL https://raw.githubusercontent.com/$GITHUB_REPO/main/install.sh | sudo bash -s -- --uninstall"
            exit 0 ;;
        *)
            print_error "Unknown option: $1"; exit 1 ;;
    esac
done

# ─── Uninstall ────────────────────────────────────────────────
if [ "$UNINSTALL" = true ]; then
    print_warning "⚠️  Starting Uninstallation..."
    for i in $(seq 1 20); do
        systemctl stop "${SERVICE_NAME}@${i}"    2>/dev/null || true
        systemctl disable "${SERVICE_NAME}@${i}" 2>/dev/null || true
    done
    systemctl stop "${SERVICE_NAME}"    2>/dev/null || true
    systemctl disable "${SERVICE_NAME}" 2>/dev/null || true
    [ -f "/etc/systemd/system/${SERVICE_NAME}@.service" ] && rm "/etc/systemd/system/${SERVICE_NAME}@.service"
    [ -f "/etc/systemd/system/${SERVICE_NAME}.service"  ] && rm "/etc/systemd/system/${SERVICE_NAME}.service"
    systemctl daemon-reload
    [ -d "$APP_DIR" ] && rm -rf "$APP_DIR"
    print_status "✅ Uninstalled successfully!"
    exit 0
fi

# Check root
if [ "$(id -u)" -ne 0 ]; then
    print_error "This script must be run as root (use sudo)"
    exit 1
fi

print_status "🚀 Starting Installation... (Workers: $WORKER_COUNT)"

# ─── System Dependencies ──────────────────────────────────────
print_status "Installing system dependencies (curl)..."
if command -v apt-get &>/dev/null; then
    apt-get update -qq
    apt-get install -y -qq curl
elif command -v yum &>/dev/null; then
    yum install -y curl
elif command -v dnf &>/dev/null; then
    dnf install -y curl
fi

for cmd in curl; do
    if ! command -v $cmd &>/dev/null; then
        print_error "$cmd not found. Please install it manually."
        exit 1
    fi
done

# ─── Stop existing services ───────────────────────────────────
print_status "Stopping existing services..."
systemctl stop ${SERVICE_NAME}@* 2>/dev/null || true
systemctl stop ${SERVICE_NAME}   2>/dev/null || true

# ─── Create app directory ─────────────────────────────────────
print_status "Creating app directory: $APP_DIR"
mkdir -p "$APP_DIR"
cd "$APP_DIR"

# ─── Download binary ──────────────────────────────────────────
ARCH=$(uname -m)
if [ "$ARCH" = "x86_64" ]; then
    BINARY="linux"
elif [ "$ARCH" = "aarch64" ]; then
    BINARY="linux-arm64"
else
    print_error "Unsupported architecture: $ARCH"
    exit 1
fi

print_status "Downloading binary ($BINARY) from latest release..."
curl -fsSL "$RELEASES_URL/$BINARY" -o "$APP_DIR/$APP_NAME"
chmod +x "$APP_DIR/$APP_NAME"
print_status "Binary downloaded."

# ─── Create .env ─────────────────────────────────────────────
# ⚠ อัพเดตอย่างเดียว (ไม่ใส่ --database-url) → คงค่า .env เดิมไว้
if [ -n "$DATABASE_URL" ] || [ ! -f "$APP_DIR/.env" ]; then
    print_status "Creating .env file..."
    cat > "$APP_DIR/.env" <<EOF
DATABASE_URL=$DATABASE_URL
PREWARM_POP=$PREWARM_POP
STORAGE_ID=$STORAGE_ID
EOF
else
    print_status "Keeping existing .env"
fi

# ─── Systemd service template ─────────────────────────────────
print_status "Creating systemd service template..."

cat > /etc/systemd/system/${SERVICE_NAME}@.service <<EOF
[Unit]
Description=Worker Prewarm %i
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/$APP_NAME
Restart=always
RestartSec=5
EnvironmentFile=$APP_DIR/.env
Environment="WORKER_ID=prewarm_$(hostname)@%i"
# SIGTERM → worker คืนงานเข้าคิว (Release) + mark ตัวเอง offline ก่อนปิด
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF

# ─── Enable & start workers ───────────────────────────────────
systemctl daemon-reload
print_status "Starting $WORKER_COUNT worker(s)..."
for i in $(seq 1 $WORKER_COUNT); do
    systemctl enable ${SERVICE_NAME}@$i
    systemctl start  ${SERVICE_NAME}@$i
    sleep 0.3
done

# ─── Verify ───────────────────────────────────────────────────
sleep 2
RUNNING=0
for i in $(seq 1 $WORKER_COUNT); do
    systemctl is-active --quiet ${SERVICE_NAME}@$i && RUNNING=$((RUNNING+1))
done

echo ""
echo "============================================"
if [ $RUNNING -eq $WORKER_COUNT ]; then
    print_status "✅ Installation completed successfully!"
else
    print_warning "$RUNNING of $WORKER_COUNT workers running — check logs below"
    journalctl -u "${SERVICE_NAME}@1" -n 15 --no-pager
fi
echo "============================================"
echo ""
echo "  Directory:  $APP_DIR"
echo "  Workers:    $RUNNING / $WORKER_COUNT running"
echo ""
echo "  Commands:"
echo "    View logs:   journalctl -u \"${SERVICE_NAME}@*\" -f"
echo "    Worker 1:    journalctl -u \"${SERVICE_NAME}@1\" -f"
echo "    Restart all: for i in \$(seq 1 $WORKER_COUNT); do systemctl restart ${SERVICE_NAME}@\$i; done"
echo "    Stop all:    for i in \$(seq 1 $WORKER_COUNT); do systemctl stop ${SERVICE_NAME}@\$i; done"
echo "    Uninstall:   curl -fsSL https://raw.githubusercontent.com/$GITHUB_REPO/main/install.sh | sudo bash -s -- --uninstall"
echo "============================================"
