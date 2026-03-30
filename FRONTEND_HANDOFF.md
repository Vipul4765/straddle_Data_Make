# Straddle Frontend Handoff

Use `straddle_Data_Make/api.py` as the frontend contract source.

## Base URL
- Local: `http://127.0.0.1:8000`
- Deployed: `http://172.105.40.96:8082` (docs: `/docs`)

## Supported symbols
- `NIFTY`, `BANKNIFTY`, `FINNIFTY`, `MIDCPNIFTY`, `SENSEX`, `BANKEX`

## API endpoints
- `GET /health`
- `GET /straddle/current/{symbol}`
- `GET /straddle/history/{symbol}?limit=20` (limit capped by `STRADDLE_API_MAX_HISTORY_LIMIT`)
- `GET /straddle/stream/{symbol}` (SSE, keepalive comments)

## Canonical payload contract (current/history/SSE update)
Minimal front-end fields (all responses include these):
```json
{
  "symbol": "NIFTY",
  "minute_int": 101500,
  "minute_str": "10:15:00",
  "strike": 22750.0,
  "ce_close": 284.55,
  "pe_close": 268.5,
  "straddle_price": 553.05,
  "volume": 12345,
  "updated_at_ms": 1774435263573,
  "version": 42
}
```

Additional fields (present, safe to ignore): `spot_price`, `atm_strike`, `ce_*`/`pe_*` OHLC, `carry_forward` flags, instrument tokens. 

Field meaning:
- `minute_int` / `minute_str`: candle minute (`HH:MM:SS`) for x-axis grouping.
- `strike`: CE/PE strike chosen for the ATM straddle.
- `straddle_price`: `ce_close + pe_close` (same as `close`).
- `updated_at_ms`: publish timestamp; combine with `minute_int` + `version` for dedupe/ordering.

## Frontend integration flow
1. On page load, fetch history (small window keeps payload light):
   - `GET /straddle/history/{symbol}?limit=60`
2. Patch latest point:
   - `GET /straddle/current/{symbol}`
3. Keep chart live with SSE (auto-reconnect via EventSource):
   - `GET /straddle/stream/{symbol}`
4. On SSE reconnect, re-pull a small history window (for example `limit=30`) and merge by `(minute_int, version)`.

## SSE event format
- Event name: `update`
- Data: canonical JSON payload above
- Heartbeat comments: `: keepalive` (no data)
- Dedupe: use tuple `(minute_int, version)`; older `version` for the same `minute_int` is stale.

## Browser example (EventSource)
```js
const symbol = "NIFTY";
const base = "http://172.105.40.96:8082";

const es = new EventSource(`${base}/straddle/stream/${symbol}`);

es.addEventListener("update", (event) => {
  const row = JSON.parse(event.data);
  // row has: symbol, minute_int/str, strike, ce_close, pe_close, straddle_price, updated_at_ms, version
  console.log("live row", row);
});

es.onerror = () => {
  // Browser auto-reconnects EventSource by default.
  console.log("SSE disconnected; waiting for reconnect...");
};
```

## Multi-symbol SSE (frontend)
Streaming is one EventSource per symbol; open multiple and merge client-side:

```js
const base = "http://172.105.40.96:8082";
const symbols = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"];

const streams = new Map();
const seen = new Map(); // symbol -> Set of `${minute_int}:${version}`

symbols.forEach((symbol) => {
  const es = new EventSource(`${base}/straddle/stream/${symbol}`);
  streams.set(symbol, es);
  seen.set(symbol, new Set());

  es.addEventListener("update", (event) => {
    const row = JSON.parse(event.data);
    const key = `${row.minute_int}:${row.version}`;
    const bucket = seen.get(symbol);
    if (bucket.has(key)) return;
    bucket.add(key);
    console.log("live", symbol, row);
    // TODO: merge into chart/table as needed
  });

  es.onerror = () => {
    console.log(`SSE disconnected for ${symbol}; waiting for reconnect...`);
  };
});
```

## Error behavior
- Invalid symbol returns `400` with allowed symbols.
- Missing data for valid symbol can return `404` on current endpoint.

## Notes for chart rendering
- Prefer `updated_at_ms` for strict ordering when duplicate `time` points appear.
- Use `time` as display label on x-axis.
- If reconnect happens, refetch `history` with a small limit (for example 20) and merge by `updated_at_ms`.
- Backend session guard: published candle times are limited to market minutes (`09:15:00` to `15:30:00` IST) with processing grace until `15:30:30`.

