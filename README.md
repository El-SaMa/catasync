
# CataSync Worker

Node.js RabbitMQ worker for CataSync offloaded imports.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/El-SaMa/catasync/main/install.sh | sudo bash
```

Then edit `/opt/catasync-worker/.env` and register the same `WORKER_NAME` and `WORKER_SECRET` in CataSync Settings > Workers.

## Required `.env`

- `RABBITMQ_URL`: RabbitMQ connection string.
- `QUEUES`: comma-separated queue names, usually `catasyncimportexecute`.
- `WP_CALLBACK_URL`: `https://example.com/wp-admin/admin-ajax.php?action=catasync_offload_callback`
- `WORKER_SECRET`: shared secret for signed callbacks and pings.
- `WORKER_NAME`: worker domain/name registered in CataSync.
- `WP_PATH`: WordPress root path available to this worker.
- `WP_BIN`: wp-cli binary, usually `wp`.

## Operations

```bash
systemctl status catasync-worker
journalctl -u catasync-worker -f
systemctl restart catasync-worker
```

## Security Rules

- Never commit `.env`, real RabbitMQ URLs, worker secrets, OpenAI keys, WordPress salts, or database credentials.
- Keep production secrets in `/opt/catasync-worker/.env` with `chmod 600`.
- Public examples must use placeholders only.
- Each worker gets its own `WORKER_NAME` and secret so CataSync can show per-worker health and revoke one worker without rotating all workers.
