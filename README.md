# CataSync Node.js Worker

A production-ready Node.js worker for CataSync offload and enrichment jobs, designed for WordPress/WooCommerce integration via RabbitMQ.

## Features
- Installs all dependencies and Node.js 18+
- Clones/updates worker repo to `/opt/niletech-catasync-worker`
- Creates a secure `.env` file for configuration
- Includes systemd service example for auto-start
- Works on Ubuntu 24+ (LXC or VM)

## Quick Install

**Recommended:**

```
curl -L https://raw.githubusercontent.com/YOURUSERNAME/catasync/main/install.sh | bash
```

Replace `YOURUSERNAME` with your GitHub username if you fork or rename the repo.

## Configuration

After install, edit `.env` in `/opt/niletech-catasync-worker`:

- `RABBITMQ_URL` — Your RabbitMQ connection string
- `QUEUES` — Comma-separated list of queues to consume
- `WP_CALLBACK_URL` — WordPress callback endpoint
- `WORKER_SECRET` — Secret for HMAC signing
- `WORKER_NAME` — (Optional) Friendly name for this worker
- `OPENAI_API_KEY` — (Optional) For enrichment jobs
- `OPENAI_MODEL` — (Optional, default: gpt-4.1-mini)

## Usage

To start the worker:

```
cd /opt/niletech-catasync-worker
npm start
```

## Run as a Service (systemd)

Create `/etc/systemd/system/catasync-worker.service`:

```
[Unit]
Description=CataSync Node.js Worker
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/niletech-catasync-worker
ExecStart=/usr/bin/npm start
Restart=always
Environment=NODE_ENV=production

[Install]
WantedBy=multi-user.target
```

Then run:
```
systemctl daemon-reload
systemctl enable --now catasync-worker
```

## Requirements
- Ubuntu 24+ (LXC, VM, or bare metal)
- Node.js 18+
- RabbitMQ instance
- WordPress plugin with CataSync offload support

## License
MIT or as specified in this repository.
