from __future__ import annotations

import asyncio
import json
import time
from functools import partial
from typing import AsyncIterator

import redis
from fastapi import FastAPI, HTTPException, Query
from starlette.responses import JSONResponse, StreamingResponse

try:
    # Package imports
    from .redis_keys import current_key, history_key, update_channel
    from .settings import (
        REDIS_URL,
        STRADDLE_API_MAX_HISTORY_LIMIT,
        STRADDLE_STREAM_HEARTBEAT_SECONDS,
        SYMBOL_CONFIG,
    )
except ImportError:  # Support direct execution: `python api.py`
    from redis_keys import current_key, history_key, update_channel
    from settings import (
        REDIS_URL,
        STRADDLE_API_MAX_HISTORY_LIMIT,
        STRADDLE_STREAM_HEARTBEAT_SECONDS,
        SYMBOL_CONFIG,
    )


ALLOWED_SYMBOLS = sorted(SYMBOL_CONFIG.keys())

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
        client = _redis_client()
        pubsub = client.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(update_channel(symbol))

        last_heartbeat = time.monotonic()
        heartbeat_every = float(STRADDLE_STREAM_HEARTBEAT_SECONDS)

        try:
            # Send an initial comment so reverse proxies flush headers.
            yield ": keepalive\n\n"

            while True:
                msg = await asyncio.to_thread(
                    partial(pubsub.get_message, ignore_subscribe_messages=True, timeout=1.0)
                )

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
        finally:
            try:
                pubsub.close()
            finally:
                client.close()

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

