
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
- `WP_EXECUTE_URL` (optional): `https://example.com/wp-admin/admin-ajax.php?action=catasync_offload_execute` (derived from callback URL if omitted).
- `WP_STATUS_URL` (optional): `https://example.com/wp-admin/admin-ajax.php?action=catasync_worker_status_ping` (derived from callback URL if omitted).
- `STATUS_PING_INTERVAL_MS` (optional): worker heartbeat interval in milliseconds (default `5000`).
- `STATUS_PING_TIMEOUT_MS` (optional): status ping HTTP timeout in milliseconds (default `30000`).
- `CALLBACK_TIMEOUT_MS` (optional): callback timeout in milliseconds (default `120000`).
- `WORKER_SECRET`: shared secret for signed callbacks and pings.
- `WORKER_NAME`: worker domain/name registered in CataSync.

This worker does not require local WordPress files. It performs imports by calling signed CataSync AJAX endpoints.

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
