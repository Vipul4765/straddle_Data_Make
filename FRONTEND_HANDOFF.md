# Frontend Handoff

Use [api.py](/home/vipul/PycharmProjects/straddle_Data_Make/api.py) as the frontend contract source.

This service now exposes 2 data families:

1. Straddle candles
2. Raw index OHLC candles

Do not treat them as the same payload type.

## Base URL

- Local: `http://127.0.0.1:8082`
- Server: `http://172.105.40.96:8082`
- Docs: `http://172.105.40.96:8082/docs`

## Supported Symbols

### Straddle symbols

- `NIFTY`
- `BANKNIFTY`
- `FINNIFTY`
- `MIDCPNIFTY`
- `SENSEX`
- `BANKEX`

These come from:

- `GET /straddle/current/{symbol}`
- `GET /straddle/history/{symbol}?limit=...`
- `GET /straddle/stream/{symbol}`

### Raw index symbols

- `INDIA_VIX`

This comes from:

- `GET /index/current/{symbol}`
- `GET /index/history/{symbol}?limit=...`
- `GET /index/stream/{symbol}`

## Important Difference

### Straddle payload

This is a synthetic candle built from:

- ATM CE
- ATM PE
- same minute

It includes straddle-specific fields like:

- `strike`
- `straddle_price`
- `ce_close`
- `pe_close`
- `spot_price`
- `version`

### INDIA_VIX payload

This is raw index OHLC data from Redis key:

- `ohlc:1m:NSE_INDEX:India_VIX`

It does not have:

- `strike`
- `straddle_price`
- `ce_close`
- `pe_close`
- `version`

It only has raw candle fields like:

- `open`
- `high`
- `low`
- `close`
- `volume`
- `token`

So if the frontend shows blank `strike` or `straddle_price` for `INDIA_VIX`, that is correct behavior.

## Endpoints

### Shared health

- `GET /health`

### Straddle endpoints

- `GET /straddle/current/{symbol}`
- `GET /straddle/history/{symbol}?limit=20`
- `GET /straddle/stream/{symbol}`

### Raw index endpoints

- `GET /index/current/{symbol}`
- `GET /index/history/{symbol}?limit=20`
- `GET /index/stream/{symbol}`

## Payload Contracts

### Straddle current/history/SSE update

Example:

```json
{
  "symbol": "NIFTY",
  "minute_int": 101500,
  "minute_str": "10:15:00",
  "spot_price": 22801.25,
  "atm_strike": 22800.0,
  "strike": 22800.0,
  "open": 210.2,
  "high": 212.4,
  "low": 208.9,
  "close": 209.9,
  "straddle_price": 209.9,
  "ce_close": 102.4,
  "pe_close": 107.5,
  "volume": 120.0,
  "updated_at_ms": 1774435263573,
  "version": 42,
  "source": "pubsub"
}
```

Important fields:

- `symbol`: chart symbol
- `minute_int`: candle minute, used for grouping
- `minute_str`: display time
- `strike`: selected strike used for the straddle candle
- `straddle_price`: same as `close`
- `spot_price`: underlying spot/index close for that minute
- `version`: update counter, useful for dedupe
- `updated_at_ms`: publish timestamp

Additional fields may exist and are safe to ignore:

- `carry_forward`
- `ce_*`
- `pe_*`
- `ce_instrument_key`
- `pe_instrument_key`
- `ce_pubsub_token`
- `pe_pubsub_token`

### INDIA_VIX current/history/SSE update

Example:

```json
{
  "symbol": "INDIA_VIX",
  "token": "NSE_INDEX:India_VIX",
  "minute_int": 105700,
  "minute_str": "10:57:00",
  "open": 28.39,
  "high": 28.40,
  "low": 28.39,
  "close": 28.40,
  "volume": 0.0,
  "source": "redis_hash"
}
```

Important fields:

- `symbol`: always `INDIA_VIX`
- `token`: source token
- `minute_int`: candle minute
- `minute_str`: display time
- `open`, `high`, `low`, `close`, `volume`: raw OHLC
- `source`: `redis_hash` or `pubsub`

Fields not present for `INDIA_VIX`:

- `strike`
- `straddle_price`
- `ce_close`
- `pe_close`
- `version`
- `updated_at_ms`

## Frontend Integration Flow

### For straddle symbols

1. Fetch history:
   - `GET /straddle/history/{symbol}?limit=60`
2. Fetch current:
   - `GET /straddle/current/{symbol}`
3. Open SSE:
   - `GET /straddle/stream/{symbol}`
4. Merge by:
   - `minute_int`
   - and prefer higher `version`

### For INDIA_VIX

1. Fetch history:
   - `GET /index/history/INDIA_VIX?limit=60`
2. Fetch current:
   - `GET /index/current/INDIA_VIX`
3. Open SSE:
   - `GET /index/stream/INDIA_VIX`
4. Merge by:
   - `minute_int`

For `INDIA_VIX`, there is no `version`, so use `minute_int` as the candle identity.

## Frontend Rules

### Straddle chart

Use:

- x-axis: `minute_int` or `minute_str`
- y-axis: `straddle_price` or `close`

### INDIA_VIX chart

Use:

- x-axis: `minute_int` or `minute_str`
- y-axis: `close`

Do not try to read:

- `strike`
- `straddle_price`
- `ce_close`
- `pe_close`

from `INDIA_VIX`.

## SSE Format

Both stream endpoints use SSE:

- event name: `update`
- heartbeat: `: keepalive`

### Straddle SSE

- endpoint: `/straddle/stream/{symbol}`
- data: full straddle JSON payload

### Index SSE

- endpoint: `/index/stream/{symbol}`
- data: raw index OHLC JSON payload

## Browser Examples

### Straddle EventSource

```js
const base = "http://172.105.40.96:8082";
const symbol = "NIFTY";

const es = new EventSource(`${base}/straddle/stream/${symbol}`);

es.addEventListener("update", (event) => {
  const row = JSON.parse(event.data);
  console.log("straddle row", row);
});
```

### INDIA_VIX EventSource

```js
const base = "http://172.105.40.96:8082";
const symbol = "INDIA_VIX";

const es = new EventSource(`${base}/index/stream/${symbol}`);

es.addEventListener("update", (event) => {
  const row = JSON.parse(event.data);
  console.log("india vix row", row);
});
```

## Recommended Frontend Normalizer

Use one normalizer in the frontend so chart code stays simple.

Example:

```js
function normalizeRow(symbol, row) {
  if (symbol === "INDIA_VIX") {
    return {
      symbol: row.symbol,
      minute_int: row.minute_int,
      minute_str: row.minute_str,
      value: row.close,
      open: row.open,
      high: row.high,
      low: row.low,
      close: row.close,
      volume: row.volume,
      type: "index",
    };
  }

  return {
    symbol: row.symbol,
    minute_int: row.minute_int,
    minute_str: row.minute_str,
    value: row.straddle_price ?? row.close,
    open: row.open,
    high: row.high,
    low: row.low,
    close: row.close,
    volume: row.volume,
    strike: row.strike,
    spot_price: row.spot_price,
    version: row.version,
    type: "straddle",
  };
}
```

## Multi-Symbol Streaming

Open one EventSource per symbol.

Recommended mapping:

- `NIFTY` -> `/straddle/stream/NIFTY`
- `BANKNIFTY` -> `/straddle/stream/BANKNIFTY`
- `FINNIFTY` -> `/straddle/stream/FINNIFTY`
- `MIDCPNIFTY` -> `/straddle/stream/MIDCPNIFTY`
- `SENSEX` -> `/straddle/stream/SENSEX`
- `BANKEX` -> `/straddle/stream/BANKEX`
- `INDIA_VIX` -> `/index/stream/INDIA_VIX`

## Error Behavior

- invalid straddle symbol -> `400`
- invalid index symbol -> `400`
- missing current data -> `404`
- SSE stays open and sends keepalive comments even if no fresh update arrives immediately

## CSV Tool Note

[straddle_live_csv.py](/home/vipul/PycharmProjects/straddle_Data_Make/straddle_live_csv.py) now supports `INDIA_VIX`.

It routes automatically:

- straddle symbols -> `/straddle/...`
- `INDIA_VIX` -> `/index/...`

That means this works:

```bash
/home/vipul/PycharmProjects/.venv/bin/python /home/vipul/PycharmProjects/straddle_Data_Make/straddle_live_csv.py --url 'http://172.105.40.96:8082/docs' --symbols INDIA_VIX --history-limit 100 --output india_vix_live.csv
```

## Final Instruction For Frontend Developer

Use:

- `/straddle/...` for `NIFTY`, `BANKNIFTY`, `FINNIFTY`, `MIDCPNIFTY`, `SENSEX`, `BANKEX`
- `/index/...` for `INDIA_VIX`

Do not try to render `INDIA_VIX` as a straddle.
