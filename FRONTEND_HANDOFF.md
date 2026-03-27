# Straddle Frontend Handoff

Use `straddle_Data_Make/api.py` as the frontend contract source.

## Base URL
- Local: `http://127.0.0.1:8000`
- Server example: `http://172.105.40.96:8001`

## Supported symbols
- `NIFTY`, `BANKNIFTY`, `FINNIFTY`, `MIDCPNIFTY`, `SENSEX`, `BANKEX`

## API endpoints
- `GET /health`
- `GET /straddle/current/{symbol}`
- `GET /straddle/history/{symbol}?limit=20`
- `GET /straddle/stream/{symbol}` (SSE)

## Canonical payload contract (current/history item/SSE update)
```json
{
  "strike": 22750.0,
  "ce_close": 284.55,
  "pe_close": 268.5,
  "straddle_price": 553.05,
  "time": "16:00:00",
  "updated_at_ms": 1774435263573
}
```

Field meaning:
- `ce_close`: selected CE close for that straddle point.
- `pe_close`: selected PE close for that straddle point.
- `strike`: selected CE/PE strike price for that candle minute.
- `straddle_price`: `ce_close + pe_close`.
- `time`: candle minute (`HH:MM:SS`), used for chart x-axis.
- `updated_at_ms`: server publish timestamp in epoch milliseconds, used for ordering/dedupe.

## Frontend integration flow
1. On page load, call history:
   - `GET /straddle/history/{symbol}?limit=60`
2. Then call current once to patch latest point:
   - `GET /straddle/current/{symbol}`
3. Keep chart live with SSE:
   - `GET /straddle/stream/{symbol}`

## SSE event format
- Event name: `update`
- Data: same canonical JSON payload above
- Heartbeat comments: `: keepalive`

## Browser example (EventSource)
```js
const symbol = "NIFTY";
const base = "http://172.105.40.96:8001";

const es = new EventSource(`${base}/straddle/stream/${symbol}`);

es.addEventListener("update", (event) => {
  const row = JSON.parse(event.data);
  // row = { strike, ce_close, pe_close, straddle_price, time, updated_at_ms }
  console.log("live row", row);
});

es.onerror = () => {
  // Browser auto-reconnects EventSource by default.
  console.log("SSE disconnected; waiting for reconnect...");
};
```

## Error behavior
- Invalid symbol returns `400` with allowed symbols.
- Missing data for valid symbol can return `404` on current endpoint.

## Notes for chart rendering
- Prefer `updated_at_ms` for strict ordering when duplicate `time` points appear.
- Use `time` as display label on x-axis.
- If reconnect happens, refetch `history` with a small limit (for example 20) and merge by `updated_at_ms`.
- Backend session guard: published candle times are limited to market minutes (`09:15:00` to `15:30:00` IST) with processing grace until `15:30:30`.

