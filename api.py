from __future__ import annotations

import asyncio
import json
import time
from functools import partial
from typing import Any, AsyncIterator

import redis
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

app = FastAPI(title="straddle_Data_Make API", version="1.0.0")


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


def _redis_client() -> redis.Redis:
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)


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

    async def event_stream() -> AsyncIterator[str]:
        # Optimize: using aioredis directly inside stream avoids thread blocking
        client = redis.asyncio.from_url(REDIS_URL, decode_responses=True)
        pubsub = client.pubsub(ignore_subscribe_messages=True)
        await pubsub.subscribe(update_channel(symbol))

        last_heartbeat = time.monotonic()
        heartbeat_every = float(STRADDLE_STREAM_HEARTBEAT_SECONDS)

        try:
            # Send an initial comment so reverse proxies flush headers.
            yield ": keepalive\n\n"

            while True:
                try:
                    msg = await asyncio.wait_for(pubsub.get_message(ignore_subscribe_messages=True), timeout=1.0)
                except asyncio.TimeoutError:
                    msg = None

                if msg and msg.get("type") == "message":
                    data = msg.get("data", "")
                    if isinstance(data, (bytes, bytearray)):
                        data = data.decode("utf-8", errors="replace")
                    # Worker publishes JSON strings. Emit as SSE "update" events.
                    yield f"event: update\ndata: {data}\n\n"
                    last_heartbeat = time.monotonic()

                now = time.monotonic()
                if now - last_heartbeat >= heartbeat_every:
                    yield ": keepalive\n\n"
                    last_heartbeat = now
        except asyncio.CancelledError:
             pass # normal when client disconnects
        except Exception as e:
            yield f"event: error\ndata: {str(e)}\n\n"
        finally:
            try:
                await pubsub.close()
            finally:
                await client.aclose()

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

    async def event_stream() -> AsyncIterator[str]:
        client = redis.asyncio.from_url(REDIS_URL, decode_responses=True)
        pubsub = client.pubsub(ignore_subscribe_messages=True)
        await pubsub.subscribe(REDIS_PUBSUB_CHANNEL)

        last_heartbeat = time.monotonic()
        heartbeat_every = float(STRADDLE_STREAM_HEARTBEAT_SECONDS)

        try:
            yield ": keepalive\n\n"

            while True:
                try:
                    msg = await asyncio.wait_for(pubsub.get_message(ignore_subscribe_messages=True), timeout=1.0)
                except asyncio.TimeoutError:
                    msg = None

                if msg and msg.get("type") == "message":
                    data = msg.get("data", "")
                    if isinstance(data, (bytes, bytearray)):
                        data = data.decode("utf-8", errors="replace")
                    parsed = _normalize_index_pubsub_event(data, symbol, token)
                    if parsed:
                        yield f"event: update\ndata: {json.dumps(parsed, ensure_ascii=True)}\n\n"
                        last_heartbeat = time.monotonic()

                now = time.monotonic()
                if now - last_heartbeat >= heartbeat_every:
                    yield ": keepalive\n\n"
                    last_heartbeat = now
        except asyncio.CancelledError:
             pass # normal when user disconnects
        except Exception as e:
            yield f"event: error\ndata: {str(e)}\n\n"
        finally:
            try:
                await pubsub.close()
            finally:
                await client.aclose()

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )