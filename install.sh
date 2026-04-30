#!/usr/bin/env bash
set -euo pipefail

# CataSync Node.js Worker installer.
# Public-safe: writes only placeholder secrets. Edit /opt/catasync-worker/.env after install.
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/El-SaMa/catasync/main/install.sh | sudo bash
#   sudo bash ./install.sh
#
# Existing /opt/catasync-worker/.env is preserved on update.

WORKER_DIR="${WORKER_DIR:-/opt/catasync-worker}"
REPO_URL="${REPO_URL:-https://github.com/El-SaMa/catasync.git}"
REPO_BRANCH="${REPO_BRANCH:-main}"
SERVICE_NAME="${SERVICE_NAME:-catasync-worker}"
SERVICE_USER="${SERVICE_USER:-catasync-worker}"
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

required_env_keys=(
  "RABBITMQ_URL"
  "QUEUES"
  "WP_CALLBACK_URL"
  "WP_STATUS_URL"
  "WORKER_SECRET"
  "WORKER_NAME"
  "WP_BIN"
  "WP_PATH"
)

if [ "$(id -u)" -ne 0 ]; then
  echo "[ERROR] Run as root, e.g. curl -fsSL .../install.sh | sudo bash" >&2
  exit 1
fi

echo "[INFO] Installing OS dependencies..."
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y ca-certificates curl git build-essential

if ! command -v node >/dev/null 2>&1 || ! node -e "process.exit(Number(process.versions.node.split('.')[0]) >= 18 ? 0 : 1)" >/dev/null 2>&1; then
  echo "[INFO] Installing Node.js 20..."
  curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
  apt-get install -y nodejs
else
  echo "[INFO] Node.js found: $(node -v)"
fi

if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
  echo "[INFO] Creating service user: $SERVICE_USER"
  useradd --system --home "$WORKER_DIR" --shell /usr/sbin/nologin "$SERVICE_USER"
fi

echo "[INFO] Installing/updating worker files into $WORKER_DIR ..."
mkdir -p "$WORKER_DIR"
if [ -n "$REPO_URL" ]; then
  echo "[INFO] Cloning/updating from $REPO_URL ($REPO_BRANCH)"
  ENV_BACKUP=""
  if [ -f "$WORKER_DIR/.env" ]; then
    ENV_BACKUP="$(mktemp)"
    cp "$WORKER_DIR/.env" "$ENV_BACKUP"
  fi
  if [ -d "$WORKER_DIR/.git" ]; then
    git -C "$WORKER_DIR" fetch --prune origin "$REPO_BRANCH"
    git -C "$WORKER_DIR" checkout "$REPO_BRANCH"
    git -C "$WORKER_DIR" reset --hard "origin/$REPO_BRANCH"
  else
    rm -rf "$WORKER_DIR"
    git clone --branch "$REPO_BRANCH" "$REPO_URL" "$WORKER_DIR"
  fi
  if [ -n "$ENV_BACKUP" ] && [ -f "$ENV_BACKUP" ]; then
    cp "$ENV_BACKUP" "$WORKER_DIR/.env"
    rm -f "$ENV_BACKUP"
  fi
else
  echo "[INFO] REPO_URL is empty; using local source sync fallback"
  if ! command -v rsync >/dev/null 2>&1; then
    apt-get install -y rsync
  fi
  rsync -a --delete \
    --exclude '.env' \
    --exclude 'node_modules' \
    --exclude '.git' \
    "$SOURCE_DIR"/ "$WORKER_DIR"/
fi

cd "$WORKER_DIR"
echo "[INFO] Installing npm dependencies..."
npm install --omit=dev

if [ ! -f .env ]; then
  echo "[INFO] Creating placeholder .env ..."
  cat > .env <<'EOF'
# RabbitMQ connection. Do not commit real values.
RABBITMQ_URL=amqp://user:pass@rabbit-host:5672/catasync

# Queue names must match CataSync Settings > Offload. Current import queue default:
QUEUES=catasyncimportexecute
PREFETCH=1

# WordPress callback endpoints.
WP_CALLBACK_URL=https://example.com/wp-admin/admin-ajax.php?action=catasync_offload_callback
# Optional override. By default this is derived from WP_CALLBACK_URL.
WP_STATUS_URL=https://example.com/wp-admin/admin-ajax.php?action=catasync_worker_status_ping
STATUS_PING_INTERVAL_MS=5000
STATUS_PING_TIMEOUT_MS=30000
CALLBACK_TIMEOUT_MS=30000
CALLBACK_MAX_ATTEMPTS=3
RECONNECT_DELAY_MS=5000

# Must match the worker secret registered in CataSync Settings > Workers.
WORKER_SECRET=change-me
WORKER_NAME=

# Local WordPress access. Worker hosts must have wp-cli and access to this WordPress install/DB.
WP_BIN=wp
WP_PATH=/var/www/html
IMPORT_TIMEOUT_MS=1200000

# Optional future enrichment settings. Keep blank unless the worker code uses them.
OPENAI_API_KEY=
OPENAI_MODEL=
EOF
  chmod 600 .env
else
  echo "[INFO] .env already exists; leaving it untouched."
fi

echo "[INFO] Checking .env keys ..."
for key in "${required_env_keys[@]}"; do
  if ! grep -q "^${key}=" .env; then
    echo "[WARN] Missing ${key} in $WORKER_DIR/.env; add it before expecting jobs to run."
  fi
done

chown -R "$SERVICE_USER:$SERVICE_USER" "$WORKER_DIR"
chmod 700 "$WORKER_DIR"
chmod 600 "$WORKER_DIR/.env"

NPM_PATH="$(command -v npm)"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
echo "[INFO] Writing systemd service: $SERVICE_FILE"
cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=CataSync RabbitMQ Worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$WORKER_DIR
ExecStart=$NPM_PATH start
Restart=always
RestartSec=5
Environment=NODE_ENV=production
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ReadWritePaths=$WORKER_DIR

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"

echo ""
echo "[INFO] Install complete."
echo "[NEXT] Edit $WORKER_DIR/.env with RabbitMQ, callback URL, worker secret, and WP_PATH."
echo "[NEXT] Register WORKER_NAME + WORKER_SECRET in CataSync Settings > Workers."
echo "[NEXT] Check status: systemctl status $SERVICE_NAME"
echo "[NEXT] Watch logs: journalctl -u $SERVICE_NAME -f"
echo "[NEXT] Future update command: curl -fsSL https://raw.githubusercontent.com/El-SaMa/catasync/main/install.sh | sudo bash"
