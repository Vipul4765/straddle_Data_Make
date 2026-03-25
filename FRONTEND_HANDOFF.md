# Straddle Frontend Handoff

## What exists today
- **Live straddle stream** is produced by `straddle_Data_Make/straddle_worker.py` from Redis `candles:all` events.
- **Historical straddle API** exists in backend at `GET /straddle/data` (`algoverve_backend_fresh/routes/straddle_routes.py`).
- These are **two different data sources/contracts**:
  - live: Redis keys/channels (worker output)
  - historical: Postgres rows via backend API (`straddles` table)

## Recommended frontend integration path
- Use backend API (`/straddle/data`) for historical pages/backfill.
- Use a backend-owned realtime bridge (WebSocket/SSE) for live updates from Redis.
- Avoid browser-direct Redis access in production.

## Redis live contract (from `straddle_worker.py`)
Per symbol (`NIFTY`, `BANKNIFTY`, `FINNIFTY`, `MIDCPNIFTY`, `SENSEX`, `BANKEX`):
- Current snapshot key: `straddle:current:{SYMBOL}`
- Rolling history key: `straddle:history:{SYMBOL}` (Redis list, capped by `STRADDLE_MAX_HISTORY`, default 1000)
- Update channel: `straddle:update:{SYMBOL}`
- Version counter: `straddle:version:{SYMBOL}`

Source OHLC hash read by worker:
- `ohlc:1m:{token}` where fields are minute ints (`"093000"` style as int strings) and `__latest__`

### Live payload shape (single JSON object)
```json
{
  "symbol": "NIFTY",
  "fo_segment": "NSE_FO",
  "expiry_rank": 1,
  "minute_int": 101500,
  "minute_str": "10:15:00",
  "spot_price": 22763.2,
  "atm_strike": 22750.0,
  "strike": 22750.0,
  "ce_pubsub_token": "NSE_FO:12345",
  "pe_pubsub_token": "NSE_FO:67890",
  "ce_carry_forward": false,
  "pe_carry_forward": true,
  "carry_forward": true,
  "ce_open": 101.2,
  "ce_high": 110.0,
  "ce_low": 99.8,
  "ce_close": 105.6,
  "pe_open": 120.4,
  "pe_high": 125.0,
  "pe_low": 118.7,
  "pe_close": 121.1,
  "open": 221.6,
  "high": 235.0,
  "low": 218.5,
  "close": 226.7,
  "volume": 0.0,
  "source": "pubsub",
  "updated_at_ms": 1760000000000,
  "version": 42
}
```

Frontend notes:
- `open/high/low/close` are **straddle OHLC = CE + PE** for selected strike.
- `carry_forward=true` means at least one leg candle was missing for this minute and forward-filled.
- `version` is monotonic per symbol and useful for dedupe/order checks.

## Backend historical API contract (`GET /straddle/data`)
Path: `algoverve_backend_fresh/routes/straddle_routes.py`

Required query params:
- `symbol`
- `filter` in:
  - `latest_full_day` or `latest`
  - `previous_latest`
  - `range_latest`
  - `range_day_plus_latest`
  - `date_plus_latest`

Optional query params by filter:
- `fromDate`, `toDate` for range filters
- `day` (`ALL`, `MON`, `TUE`, `WED`, `THU`, `FRI`, `SAT`, `SUN`)
- `date` for `date_plus_latest`

Each row payload (from `schemas/straddle_schema.py`):
- `trade_date`, `trade_time`, `open`, `high`, `low`, `close`, `atm`, `ce_price`, `pe_price`, `straddle_price`, `indiavix`

## Readiness assessment
- **Good**: core live computation pipeline is clear and production-usable for Redis consumers.
- **Good**: historical API is documented with typed response schema.
- **Gap**: no dedicated backend realtime endpoint for frontend; live stream is Redis-only today.
- **Gap**: `straddle_routes.py` has `#TODO secure this straddle api`.
- **Gap**: no tests in `straddle_Data_Make/` (manual validation only at present).

## Minimum backend tasks before frontend starts realtime UI
1. Add backend WebSocket/SSE endpoint that relays `straddle:update:{SYMBOL}`.
2. Freeze a single frontend contract (field names/types for live + historical merge).
3. Define reconnect/backfill policy (on reconnect: fetch `straddle:current` + last N history points).

