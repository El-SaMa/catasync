#!/bin/bash
set -e

# CataSync Node.js Worker Install Script
# Usage: curl -L https://raw.githubusercontent.com/El-SaMa/catasync/main/install.sh | bash


echo "[INFO] Installing dependencies (curl, git, build-essential)..."
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y curl git build-essential


echo "[INFO] Checking Node.js..."
if ! command -v node >/dev/null 2>&1; then
  echo "[INFO] Node.js not found, installing Node.js 20..."
  curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
  apt-get install -y nodejs
else
  echo "[INFO] Node.js found: $(node -v)"
fi


echo "[INFO] Cloning or updating worker repo..."
WORKER_DIR="/opt/catasync-worker"
REPO_URL="https://github.com/El-SaMa/catasync.git"
if [ -d "$WORKER_DIR/.git" ]; then
  cd "$WORKER_DIR"
  git pull --rebase
else
  git clone "$REPO_URL" "$WORKER_DIR"
  cd "$WORKER_DIR"
fi
cd "$WORKER_DIR"

# 4. Install npm dependencies
echo "[INFO] Installing npm dependencies..."
if ! npm install --omit=dev; then
  echo "[ERROR] npm install failed. Check your network, Node.js version, and permissions." >&2
  exit 1
fi

# 5. Create .env if missing
if [ ! -f .env ]; then
  echo "[INFO] Creating .env file..."
  cat > .env <<EOF
# RabbitMQ
RABBITMQ_URL=amqp://user:pass@host:5672/vhost
# Comma-separated list of queues to consume
QUEUES=catasync.import.execute,catasync.import.enrich
# WordPress callback URL
WP_CALLBACK_URL=https://niletech.fi/wp-admin/admin-ajax.php?action=wave_woo_offload_callback
# Worker secret for HMAC
WORKER_SECRET=changeme
# Worker name (optional)
WORKER_NAME=
# OpenAI API key (optional, for enrichment)
OPENAI_API_KEY=
# OpenAI model (optional, default: gpt-4.1-mini)
OPENAI_MODEL=gpt-4.1-mini
EOF
  chmod 600 .env
  echo "[INFO] .env created (chmod 600). Please edit with your RabbitMQ, WP, and OpenAI details."
else
  echo "[INFO] .env already exists, not overwriting."
fi

# 6. Print next steps
echo ""
echo "[INFO] Install complete!"
echo "To start the worker:"
echo "  cd $WORKER_DIR"
echo "  npm start"
echo ""
echo "Edit .env to configure RabbitMQ, queues, callback URL, and API keys."
echo ""
 # (Optional) To run as a service, create /etc/systemd/system/catasync-worker.service:
# [Unit]
# Description=CataSync Worker
# After=network.target
#
# [Service]
# Type=simple
# User=root
# WorkingDirectory=$WORKER_DIR
# ExecStart=/usr/bin/npm start
# Restart=always
# Environment=NODE_ENV=production
#
# [Install]
# WantedBy=multi-user.target
echo ""
