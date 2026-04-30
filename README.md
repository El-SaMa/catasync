
# CataSync Worker

Node.js RabbitMQ worker for CataSync offloaded imports.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/El-SaMa/catasync/main/install.sh | sudo bash
```

Then edit `/opt/catasync-worker/.env` and register the same `WORKER_NAME` and `WORKER_SECRET` in CataSync Settings > Workers.
Re-run the same command to update a worker from GitHub. Existing `/opt/catasync-worker/.env` is preserved.

## Required `.env`

- `RABBITMQ_URL`: RabbitMQ connection string.
- `QUEUES`: comma-separated queue names, usually `catasyncimportexecute`.
- `WP_CALLBACK_URL`: `https://example.com/wp-admin/admin-ajax.php?action=catasync_offload_callback`
- `WP_STATUS_URL` (optional): `https://example.com/wp-admin/admin-ajax.php?action=catasync_worker_status_ping` (derived from callback URL if omitted).
- `WP_BIN`: wp-cli binary, usually `wp`.
- `WP_PATH`: local WordPress root available to this worker.
- `IMPORT_TIMEOUT_MS` (optional): max local wp-cli import duration in milliseconds (default `1200000`).
- `STATUS_PING_INTERVAL_MS` (optional): worker heartbeat interval in milliseconds (default `5000`).
- `STATUS_PING_TIMEOUT_MS` (optional): status ping HTTP timeout in milliseconds (default `30000`).
- `CALLBACK_TIMEOUT_MS` (optional): callback timeout in milliseconds (default `30000`).
- `CALLBACK_MAX_ATTEMPTS` (optional): callback retry attempts (default `3`).
- `WORKER_SECRET`: shared secret for signed callbacks and pings.
- `WORKER_NAME`: worker domain/name registered in CataSync.

This worker runs imports locally through wp-cli. The worker host must have filesystem and database access to the WordPress install configured by `WP_PATH`.

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
