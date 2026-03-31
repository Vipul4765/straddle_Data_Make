# straddle_Data_Make

Nearest-expiry straddle utilities built around the live Redis pub/sub payloads on `candles:all`.

## Files

- `README.md`
  Run commands and quick usage.
- `__init__.py`
  Keeps package import support.
- `contract_data.py`
  Downloads Upstox contracts and keeps only expiry rank `1` for `FUT`, `CE`, `PE`.
- `settings.py`
  Shared runtime/env configuration loader used by the worker, API, client, and contract downloader.
- `symbol_config.json`
  External symbol metadata so spot/FO mappings and strike steps are not hardcoded in Python files.
- `straddle_builder.py`
  Builds straddle selections from `segment_wise_contract_dict`.
- `redis_keys.py`
  Redis key names used by the worker and client.
- `straddle_worker.py`
  Shared worker that builds real CE+PE straddle candles, stores the latest snapshot in Redis, and appends chart history.
- `straddle_client.py`
  Reads the current shared snapshot, chart history, or watches symbol update channels.
- `api.py`
  Tiny FastAPI wrapper that exposes current/history over HTTP and update stream over SSE.

## Core Contract Path

For NIFTY:

```python
segment_wise_contract_dict["NSE_FO"]["NIFTY"]["CE"][1][22750.0]["instrument_key"]
segment_wise_contract_dict["NSE_FO"]["NIFTY"]["PE"][1][22750.0]["instrument_key"]
```

## Test Setup

Use your existing SSH tunnel:

```bash
ssh -fN -L 6380:127.0.0.1:6379 root@<server-host>
```

Put Redis credentials in `.env` before testing:

```dotenv
REDIS_PASSWORD=<paste-current-redis-password-here>
REDIS_URL=redis://:${REDIS_PASSWORD}@127.0.0.1:6380/0
```

All scripts in this folder auto-load `.env` from `straddle_Data_Make/`.

Symbol mappings are loaded from `symbol_config.json` (or the file pointed to by `STRADDLE_SYMBOL_CONFIG_FILE`).

Optional env keys for contract downloader:

```bash
export STRADDLE_CONTRACT_URL="https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
export STRADDLE_CONTRACT_OUTPUT_FILE="/home/vipul/PycharmProjects/straddle_Data_Make/pub_sub_tokens.txt"
```

## Generate Token File

```bash
/home/vipul/PycharmProjects/.venv/bin/python -m straddle_Data_Make.contract_data
```

## Build One Straddle In Code

```python
from straddle_Data_Make.contract_data import contract_file
from straddle_Data_Make.straddle_builder import build_atm_straddle

segment_wise_contract_dict, _ = contract_file()
straddle = build_atm_straddle(segment_wise_contract_dict, "NIFTY", 22763.2)
print(straddle.to_dict())
```

## Best Low-Load Server Mode

Run one shared worker on the server:

```bash
/home/vipul/PycharmProjects/.venv/bin/python /home/vipul/PycharmProjects/straddle_Data_Make/straddle_worker.py \
  --redis-url "$REDIS_URL" \
  --startup-backfill-candles 0 \
  --symbols NIFTY,BANKNIFTY,FINNIFTY,MIDCPNIFTY,SENSEX,BANKEX
```

Startup backfill:

- On worker start, recent candles are rebuilt from Redis source hashes before live streaming.
- Default is `0`, which means backfill **all available spot minutes** per symbol.
- Backfill is session-safe: only `09:15:00` to current session minute (max `15:30:00`) is rebuilt.
- Set a positive number (for example `100`) to cap startup backfill size.
- Configure with env `STRADDLE_STARTUP_BACKFILL_CANDLES` or CLI `--startup-backfill-candles`.
- At/after `09:15 IST`, worker clears previous-day `straddle:current:*` and `straddle:history:*` once per day and waits for fresh spot pub/sub before republishing.
- Live processing safety window is `09:15:00` to `15:30:30 IST`; candles published remain in market-minute range (`09:15:00` to `15:30:00`).

What it does:

- listens to `candles:all`
- bootstraps from Redis `ohlc:1m:<token>` hashes
- aligns spot, CE, and PE by minute
- builds real straddle OHLC from `CE + PE`
- stores latest snapshot in Redis
- stores history for chart use
- publishes updates on `straddle:update:<SYMBOL>`

Redis keys:

- `straddle:current:NIFTY`
- `straddle:history:NIFTY`
- `straddle:current:BANKNIFTY`
- `straddle:history:BANKNIFTY`
- `straddle:update:NIFTY`
- `straddle:update:BANKNIFTY`

Read current snapshot:

```bash
/home/vipul/PycharmProjects/.venv/bin/python /home/vipul/PycharmProjects/straddle_Data_Make/straddle_client.py \
  --redis-url "$REDIS_URL" \
  current --symbol NIFTY
```

Read chart history:

```bash
/home/vipul/PycharmProjects/.venv/bin/python /home/vipul/PycharmProjects/straddle_Data_Make/straddle_client.py \
  --redis-url "$REDIS_URL" \
  history --symbol NIFTY --limit 20
```

Watch updates:

```bash
/home/vipul/PycharmProjects/.venv/bin/python /home/vipul/PycharmProjects/straddle_Data_Make/straddle_client.py \
  --redis-url "$REDIS_URL" \
  watch --symbol NIFTY
```

## HTTP API for Frontend (FastAPI + SSE)

Install API dependencies (server once):

```bash
/home/vipul/PycharmProjects/.venv/bin/pip install -r /home/vipul/PycharmProjects/straddle_Data_Make/requirements.txt
```

Run API using the same `.env` values:

```bash
cd /home/vipul/PycharmProjects/straddle_Data_Make
set -a
source .env
set +a
/home/vipul/PycharmProjects/.venv/bin/uvicorn api:app --host 0.0.0.0 --port 8000
```

Endpoints:

- `GET /health`
- `GET /straddle/current/{symbol}`
- `GET /straddle/history/{symbol}?limit=20`
- `GET /straddle/stream/{symbol}` (SSE)

Response shape (current/history item/SSE update):

```json
{
  "strike": 22750.0,
  "ce_close": 284.55,
  "pe_close": 268.5,
  "straddle_price": 553.05,
  "time": "10:15:00",
  "updated_at_ms": 1760000000000
}
```

Examples:

```bash
curl -sS http://127.0.0.1:8000/health
curl -sS http://127.0.0.1:8000/straddle/current/NIFTY
curl -sS "http://127.0.0.1:8000/straddle/history/NIFTY?limit=5"
curl -N http://127.0.0.1:8000/straddle/stream/NIFTY
```

Swagger docs:

```text
http://<SERVER_IP>:8000/docs
```

## Scaling Improvement Docs

- `SSE_SCALING.md` - required architecture and deployment improvements for SSE at scale.
- `SSE_SERVER_TUNING_CHECKLIST.md` - quick production checklist (app, Nginx, OS limits, validation).

## Recent Production Resiliency Updates

Recent infrastructural upgrades were deployed specifically targeting high-scale durability, holiday calendar automation, and enterprise grade system resiliency:

1. **Enterprise Redis In-Memory Fan-Out (SSE Streams)**  
   - **Problem:** Streaming active live markets to over 10K frontend clients simultaneously blew up the Redis network sockets via per-user 1:1 connections. 
   - **Solution:** Integrated an automated `PubSubManager` in `api.py`. The node establishes strictly **ONE** master Redis socket for market updates. When a tick drops, the python system internally broadcasts the JSON via massive asynchronous memory-bound user queues.
   - **Scale:** Effortlessly scales 65,000+ SSE streams concurrent instances per instance while respecting internal server boundaries. 

2. **Aggressive Zombie Memory Protection**  
   - **Problem:** Rapid "rage-reloads" or flaky internet users generated hundreds of abandoned `CancelError` sockets before python could trigger garbage collection unsubscribes, silently stacking memory leaks leading to Out-Of-Memory (OOM) outages.
   - **Solution:** Deployed nested `asyncio.shield(...)` blocks across all SSE destruction calls. No matter how violently a user connection slices or crashes, memory sweep sequences *must* finish natively guaranteeing zero runaway memory consumption. Added bounded max-items queues which safely discard/skip trailing users to prevent stale memory overflow.

3. **Persistent Thread Pools for Synchronous REST (GET) Bombs**
   - **Problem:** Generating standard TCP sessions `redis.from_url` per `/get/current` request during peak market hours crushed Linux bounds causing massive `TIME_WAIT` spikes—blocking the entire host access for 60-seconds. 
   - **Solution:** Replaced dynamic instantiations with a globally persistent `_SYNC_REDIS_POOL`. 

4. **Dynamic Database Trading Day Validation**  
   - Transferred purely static `settings.py` dates to a strictly mirrored PostgreSQL implementation tied to the main engine stack (`market_ingest_final`). 
   - Now checks `SELECT exchange FROM holidays WHERE date = 'Today'` perfectly bridging weekend vs non-weekend database trading exceptions dynamically. 
   - **Fail-Open Fallback Engine:** Graceful database drops log error exceptions temporarily isolating the cycle without caching fatal server death flags, enabling the node to automatically jump back alive the moment Postgres re-boots.

## Configuration
Set environment variables directly or create a `.env` file referencing your unified `market_ingest_final` db setups.

`DATABASE_URL=postgresql://user:password@cloud-url`
`REDIS_URL=redis://localhost:6379/0`
`REDIS_PUBSUB_CHANNEL=candles:all`

## Standard Deployment / Operation

### Running on Production Servers (Background / nohup)
To run these securely on your VPS instance without them dying when you close the terminal, you must use background `nohup` execution and bind the API explicitly to `0.0.0.0` for Nginx reverse-proxy pairing.

1. **Move to directory and create log target:**
```bash
cd /home/vipul/PycharmProjects/straddle_Data_Make
mkdir -p ../logs
```

2. **Load `.env` securely and start Background Worker:**
```bash
set -a && source .env && set +a
nohup python3 straddle_worker.py > ../logs/straddle_worker.out 2>&1 &
```

3. **Start the API Server tightly bound to `0.0.0.0`:**
```bash
nohup uvicorn api:app --host 0.0.0.0 --port 8000 > ../logs/straddle_api.out 2>&1 &
```

*(To monitor these later, run `tail -f ../logs/straddle_worker.out` or `tail -f ../logs/straddle_api.out`)*

### Quick Development Run
If you are just developing, you can run normal mode:
```bash
# Run Worker
python3 straddle_worker.py

# Run API
uvicorn api:app --host 0.0.0.0 --port 8000
```