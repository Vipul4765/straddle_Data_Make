from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager
from functools import partial
from typing import Any, AsyncIterator, Dict, Set

import redis
import redis.asyncio
from fastapi import FastAPI, HTTPException, Query
from starlette.responses import JSONResponse, StreamingResponse

try:
    # Package imports
    from .redis_keys import current_key, history_key, source_hash_key, update_channel
    from .settings import (
        REDIS_PUBSUB_CHANNEL,
        REDIS_URL,
        STRADDLE_API_MAX_HISTORY_LIMIT,
        STRADDLE_STREAM_HEARTBEAT_SECONDS,
        SYMBOL_CONFIG,
    )
except ImportError:  # Support direct execution: `python api.py`
    from redis_keys import current_key, history_key, source_hash_key, update_channel
    from settings import (
        REDIS_PUBSUB_CHANNEL,
        REDIS_URL,
        STRADDLE_API_MAX_HISTORY_LIMIT,
        STRADDLE_STREAM_HEARTBEAT_SECONDS,
        SYMBOL_CONFIG,
    )


ALLOWED_SYMBOLS = sorted(SYMBOL_CONFIG.keys())
RAW_INDEX_SYMBOLS = {
    "INDIA_VIX": {
        "pubsub_token": "NSE_INDEX:India_VIX",
        "display_name": "India VIX",
    }
}
ALLOWED_INDEX_SYMBOLS = sorted(RAW_INDEX_SYMBOLS.keys())

class PubSubManager:
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self._redis_client: redis.asyncio.Redis | None = None
        self._pubsub = None
        
        # Channel name -> set of asyncio.Queue
        self.listeners: Dict[str, Set[asyncio.Queue]] = {}
        self._listen_task: asyncio.Task | None = None

    async def get_redis(self):
        if self._redis_client is None:
            self._redis_client = redis.asyncio.from_url(self.redis_url, decode_responses=True)
        return self._redis_client

    async def start(self):
        if self._listen_task is not None:
            return
        r = await self.get_redis()
        self._pubsub = r.pubsub(ignore_subscribe_messages=True)
        self._listen_task = asyncio.create_task(self._listen_loop())

    async def stop(self):
        if self._listen_task:
            self._listen_task.cancel()
            self._listen_task = None
        if self._pubsub:
            await self._pubsub.close()
            self._pubsub = None
        if self._redis_client:
            await self._redis_client.aclose()
            self._redis_client = None

    async def _listen_loop(self):
        # Keeps reading from a single shared redis connection
        try:
            while True:
                if not self._pubsub.subscribed:
                    await asyncio.sleep(0.1)
                    continue

                msg = await self._pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg and msg.get("type") == "message":
                    channel = msg.get("channel", "")
                    data = msg.get("data", "")
                    if isinstance(data, (bytes, bytearray)):
                        data = data.decode("utf-8", errors="replace")
                        
                    if channel in self.listeners:
                        queues = list(self.listeners[channel])
                        for q in queues:
                            try:
                                q.put_nowait(data)
                            except asyncio.QueueFull:
                                # Drop slow clients to prevent memory leaks
                                pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"PubSubManager listen loop error: {e}")

    async def subscribe(self, channel: str) -> asyncio.Queue:
        if self._pubsub is None:
            await self.start()

        if channel not in self.listeners:
            self.listeners[channel] = set()
            await self._pubsub.subscribe(channel)

        # Buffer up to 100 messages for slow clients
        q = asyncio.Queue(maxsize=100)
        self.listeners[channel].add(q)
        return q

    async def unsubscribe(self, channel: str, q: asyncio.Queue):
        if channel in self.listeners and q in self.listeners[channel]:
            self.listeners[channel].remove(q)
            if not self.listeners[channel]:
                del self.listeners[channel]
                if self._pubsub:
                    await self._pubsub.unsubscribe(channel)


broadcaster = PubSubManager(REDIS_URL)

@asynccontextmanager
async def lifespan(application: FastAPI):
    # Startup
    await broadcaster.start()
    yield
    # Shutdown
    await broadcaster.stop()
    _SYNC_REDIS_POOL.disconnect()

app = FastAPI(title="straddle_Data_Make API", version="1.0.0", lifespan=lifespan)

def _validate_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if normalized not in SYMBOL_CONFIG:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_symbol",
                "symbol": symbol,
                "allowed_symbols": ALLOWED_SYMBOLS,
            },
        )
    return normalized


# --- Sync Connection Pool for GET Requests ---
_SYNC_REDIS_POOL = redis.ConnectionPool.from_url(REDIS_URL, decode_responses=True)

def _redis_client() -> redis.Redis:
    return redis.Redis(connection_pool=_SYNC_REDIS_POOL)


def _validate_index_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if normalized not in RAW_INDEX_SYMBOLS:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_index_symbol",
                "symbol": symbol,
                "allowed_symbols": ALLOWED_INDEX_SYMBOLS,
            },
        )
    return normalized


def _minute_str(minute_int: int) -> str:
    return f"{minute_int // 10000:02d}:{(minute_int // 100) % 100:02d}:00"


def _parse_latest_hash_payload(raw: str | None, token: str, symbol: str) -> dict[str, Any] | None:
    if not raw:
        return None
    values = json.loads(raw)
    if not isinstance(values, list) or len(values) < 6:
        return None
    minute_int = int(values[5])
    return {
        "symbol": symbol,
        "token": token,
        "minute_int": minute_int,
        "minute_str": _minute_str(minute_int),
        "open": float(values[0]),
        "high": float(values[1]),
        "low": float(values[2]),
        "close": float(values[3]),
        "volume": float(values[4]),
        "source": "redis_hash",
    }


def _parse_history_hash_payload(
    raw: str | None,
    token: str,
    symbol: str,
    minute_int: int,
) -> dict[str, Any] | None:
    if not raw:
        return None
    values = json.loads(raw)
    if not isinstance(values, list) or len(values) < 4:
        return None
    return {
        "symbol": symbol,
        "token": token,
        "minute_int": minute_int,
        "minute_str": _minute_str(minute_int),
        "open": float(values[0]),
        "high": float(values[1]),
        "low": float(values[2]),
        "close": float(values[3]),
        "volume": float(values[4]) if len(values) > 4 else 0.0,
        "source": "redis_hash",
    }


def _normalize_index_pubsub_event(raw: str, symbol: str, token: str) -> dict[str, Any] | None:
    event = json.loads(raw)
    if str(event.get("token") or "") != token:
        return None
    minute_int = int(event.get("minute") or event.get("minute_int") or 0)
    return {
        "symbol": symbol,
        "token": token,
        "minute_int": minute_int,
        "minute_str": str(event.get("minute_str") or _minute_str(minute_int)),
        "open": float(event["open"]),
        "high": float(event["high"]),
        "low": float(event["low"]),
        "close": float(event["close"]),
        "volume": float(event.get("volume") or 0),
        "source": "pubsub",
    }


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/straddle/current/{symbol}")
def get_straddle_current(symbol: str) -> JSONResponse:
    symbol = _validate_symbol(symbol)
    client = _redis_client()
    try:
        raw = client.get(current_key(symbol))
    finally:
        client.close()

    if not raw:
        raise HTTPException(
            status_code=404,
            detail={"error": "missing_current", "symbol": symbol},
        )

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=500,
            detail={"error": "corrupt_current_json", "symbol": symbol},
        )
    return JSONResponse(payload)


@app.get("/straddle/history/{symbol}")
def get_straddle_history(
    symbol: str,
    limit: int = Query(20, ge=1),
) -> JSONResponse:
    symbol = _validate_symbol(symbol)
    limit = min(limit, int(STRADDLE_API_MAX_HISTORY_LIMIT))

    client = _redis_client()
    try:
        start = -limit
        rows = client.lrange(history_key(symbol), start, -1)
    finally:
        client.close()

    out: list[dict] = []
    for row in rows:
        try:
            out.append(json.loads(row))
        except json.JSONDecodeError:
            # Keep the API resilient: ignore corrupt rows rather than failing the whole response.
            continue
    return JSONResponse(out)


@app.get("/straddle/stream/{symbol}")
async def stream_straddle(symbol: str) -> StreamingResponse:
    symbol = _validate_symbol(symbol)
    channel = update_channel(symbol)

    async def event_stream() -> AsyncIterator[str]:
        q = await broadcaster.subscribe(channel)

        last_heartbeat = time.monotonic()
        heartbeat_every = float(STRADDLE_STREAM_HEARTBEAT_SECONDS)

        try:
            yield ": keepalive\n\n"

            while True:
                time_to_wait = max(0.0, heartbeat_every - (time.monotonic() - last_heartbeat))
                
                try:
                    data = await asyncio.wait_for(q.get(), timeout=min(1.0, time_to_wait))
                    yield f"event: update\ndata: {data}\n\n"
                    last_heartbeat = time.monotonic()
                except asyncio.TimeoutError:
                    now = time.monotonic()
                    if now - last_heartbeat >= heartbeat_every:
                        yield ": keepalive\n\n"
                        last_heartbeat = now
        except (asyncio.CancelledError, Exception) as e:
             pass # normal when client disconnects
        finally:
            # Shield removal to prevent cancelled task from killing the unsubscribe hook
            try:
                await asyncio.shield(broadcaster.unsubscribe(channel, q))
            except Exception:
                pass

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            # For nginx: do not buffer SSE.
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/index/current/{symbol}")
def get_index_current(symbol: str) -> JSONResponse:
    symbol = _validate_index_symbol(symbol)
    token = RAW_INDEX_SYMBOLS[symbol]["pubsub_token"]

    client = _redis_client()
    try:
        raw = client.hget(source_hash_key(token), "__latest__")
    finally:
        client.close()

    payload = _parse_latest_hash_payload(raw, token, symbol)
    if not payload:
        raise HTTPException(
            status_code=404,
            detail={"error": "missing_current", "symbol": symbol, "token": token},
        )
    return JSONResponse(payload)


@app.get("/index/history/{symbol}")
def get_index_history(
    symbol: str,
    limit: int = Query(20, ge=1),
) -> JSONResponse:
    symbol = _validate_index_symbol(symbol)
    token = RAW_INDEX_SYMBOLS[symbol]["pubsub_token"]
    limit = min(limit, int(STRADDLE_API_MAX_HISTORY_LIMIT))

    client = _redis_client()
    try:
        raw_fields = client.hkeys(source_hash_key(token))
        minute_values = sorted(
            int(raw_field)
            for raw_field in raw_fields
            if raw_field and raw_field != "__latest__" and str(raw_field).isdigit()
        )
        selected_minutes = minute_values[-limit:]
        rows = client.hmget(source_hash_key(token), [str(minute) for minute in selected_minutes]) if selected_minutes else []
    finally:
        client.close()

    out: list[dict[str, Any]] = []
    for minute_int, raw in zip(selected_minutes, rows):
        parsed = _parse_history_hash_payload(raw, token, symbol, minute_int)
        if parsed:
            out.append(parsed)
    return JSONResponse(out)


@app.get("/index/stream/{symbol}")
async def stream_index(symbol: str) -> StreamingResponse:
    symbol = _validate_index_symbol(symbol)
    token = RAW_INDEX_SYMBOLS[symbol]["pubsub_token"]
    channel = REDIS_PUBSUB_CHANNEL

    async def event_stream() -> AsyncIterator[str]:
        q = await broadcaster.subscribe(channel)

        last_heartbeat = time.monotonic()
        heartbeat_every = float(STRADDLE_STREAM_HEARTBEAT_SECONDS)

        try:
            yield ": keepalive\n\n"

            while True:
                time_to_wait = max(0.0, heartbeat_every - (time.monotonic() - last_heartbeat))

                try:
                    raw_data = await asyncio.wait_for(q.get(), timeout=min(1.0, time_to_wait))
                    parsed = _normalize_index_pubsub_event(raw_data, symbol, token)
                    if parsed:
                        yield f"event: update\ndata: {json.dumps(parsed, ensure_ascii=True)}\n\n"
                        last_heartbeat = time.monotonic()
                except asyncio.TimeoutError:
                    now = time.monotonic()
                    if now - last_heartbeat >= heartbeat_every:
                        yield ": keepalive\n\n"
                        last_heartbeat = now
        except (asyncio.CancelledError, Exception) as e:
             pass # normal when user disconnects
        finally:
            try:
                await asyncio.shield(broadcaster.unsubscribe(channel, q))
            except Exception:
                pass

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )