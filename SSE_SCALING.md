# SSE Scaling Improvements (Must-Do)

This document defines the required production improvements for `straddle_Data_Make/api.py` SSE scalability.

## 1) One Redis Subscriber Per Symbol (Fan-In)

Current bottleneck pattern is one Redis pub/sub per client connection.

Target pattern:
- Maintain exactly one Redis subscriber per symbol per API process.
- Read updates from Redis once.
- Broadcast to all connected SSE clients in memory.

Why:
- Reduces Redis connection count drastically.
- Removes per-client Redis polling overhead.

## 2) In-Memory Fanout to SSE Clients

Target pattern:
- Keep client registries in process memory keyed by symbol.
- For each symbol update, push payload to all client queues.
- Use bounded `asyncio.Queue` per client.

Why:
- Horizontal fanout cost moves from Redis to app memory/event loop.
- Better control over slow consumers.

## 3) Backpressure Policy (Required)

Each client queue must be bounded (for example, size 10-50).

Recommended behavior:
- If queue is full, close/drop that slow client.
- Do not block the broadcaster.
- Track drop count metrics.

Why:
- Prevents one slow client from degrading all clients.

## 4) Heartbeat Tuning

Set heartbeat to 15-30 seconds.

Recommended:
- `STRADDLE_STREAM_HEARTBEAT_SECONDS=20`
- Keepalive comment frame: `: keepalive\n\n`

Why:
- Too-low heartbeat increases overhead.
- Too-high heartbeat can trigger proxy idle disconnects.

## 5) Nginx in Front of FastAPI (SSE Proxy Tuning)

Use Nginx reverse proxy for buffering/timeout control.

Example server block snippet:

```nginx
location /straddle/stream/ {
    proxy_pass http://127.0.0.1:8000;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_set_header Host $host;

    proxy_buffering off;
    proxy_cache off;
    gzip off;

    proxy_read_timeout 3600s;
    proxy_send_timeout 3600s;
    send_timeout 3600s;
}
```

Why:
- Prevents buffering SSE frames.
- Keeps long-lived stream connections stable.

## 6) Raise OS Limits on Server

Increase file descriptor and socket limits for high connection counts.

Recommended baseline:
- `ulimit -n` >= 65535
- `net.core.somaxconn=4096`

Example runtime checks:

```bash
ulimit -n
sysctl net.core.somaxconn
```

Example sysctl apply:

```bash
sudo sysctl -w net.core.somaxconn=4096
```

Persist settings in:
- `/etc/sysctl.conf` (kernel params)
- `/etc/security/limits.conf` (fd limits)

## Rollout Order

1. Implement one-subscriber-per-symbol fan-in and memory fanout.
2. Add bounded queue backpressure and slow-client disconnect policy.
3. Tune heartbeat to 15-30 seconds.
4. Put Nginx in front with SSE-safe proxy settings.
5. Raise OS limits.
6. Run load tests again and publish new max concurrency.

