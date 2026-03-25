# SSE Server Tuning Checklist

Use this checklist during deployment of `straddle_Data_Make` SSE API.

## App-Level

- [ ] One Redis subscriber per symbol in API process.
- [ ] In-memory fanout to all SSE clients (no per-client Redis subscription).
- [ ] Bounded queue per client (10-50) to enforce backpressure.
- [ ] Drop/close clients that cannot consume fast enough.
- [ ] Heartbeat configured between 15-30s.

## Nginx-Level

- [ ] `proxy_buffering off` for `/straddle/stream/*`.
- [ ] `proxy_http_version 1.1` enabled.
- [ ] `proxy_read_timeout` >= 3600s.
- [ ] `proxy_send_timeout` >= 3600s.
- [ ] `gzip off` for stream route.

## OS-Level

- [ ] `ulimit -n` increased (target >= 65535).
- [ ] `net.core.somaxconn` increased (target >= 4096).
- [ ] Limits persisted across reboot.

## Verification Commands

```bash
ulimit -n
sysctl net.core.somaxconn
curl -sS http://127.0.0.1:8000/health
curl -N http://127.0.0.1:8000/straddle/stream/NIFTY
```

## Success Criteria

- No Redis connection explosion during SSE load test.
- Stable stream connections for target concurrent clients.
- Slow-client drops are visible in logs/metrics.
- API latency remains stable for `/straddle/current/{symbol}` and `/straddle/history/{symbol}`.

