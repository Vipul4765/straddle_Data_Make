from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import redis
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

try:
    from .contract_data import SYMBOL_CONFIG
    from .redis_keys import current_key, history_key, update_channel
    from .settings import (
        REDIS_URL,
        STRADDLE_API_MAX_HISTORY_LIMIT,
        STRADDLE_STREAM_HEARTBEAT_SECONDS,
    )
except ImportError:  # Support direct script execution
    from contract_data import SYMBOL_CONFIG
    from redis_keys import current_key, history_key, update_channel
    from settings import REDIS_URL, STRADDLE_API_MAX_HISTORY_LIMIT, STRADDLE_STREAM_HEARTBEAT_SECONDS

DEFAULT_REDIS_URL = REDIS_URL
DEFAULT_STREAM_HEARTBEAT_SECONDS = STRADDLE_STREAM_HEARTBEAT_SECONDS
MAX_HISTORY_LIMIT = STRADDLE_API_MAX_HISTORY_LIMIT

app = FastAPI(title="Straddle Live API", version="1.0.0")


def _time_from_payload(payload: dict[str, Any]) -> str | None:
    minute_str = payload.get("minute_str")
    if minute_str:
        return str(minute_str)

    minute_int = payload.get("minute_int")
    if minute_int is None:
        return None

    try:
        minute_int_value = int(minute_int)
    except (TypeError, ValueError):
        return None

    hour = minute_int_value // 10000
    minute = (minute_int_value // 100) % 100
    return f"{hour:02d}:{minute:02d}:00"


def _to_user_payload(payload: dict[str, Any]) -> dict[str, Any]:
    # Keep a stable frontend contract with explicit candle time + publish time.
    return {
        "ce_close": payload.get("ce_close"),
        "pe_close": payload.get("pe_close"),
        "straddle_price": payload.get("straddle_price", payload.get("close")),
        "time": _time_from_payload(payload),
        "updated_at_ms": payload.get("updated_at_ms"),
    }


def _normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if normalized not in SYMBOL_CONFIG:
        allowed = ", ".join(sorted(SYMBOL_CONFIG))
        raise HTTPException(status_code=400, detail=f"Unsupported symbol '{symbol}'. Allowed: {allowed}")
    return normalized


def _get_redis_client() -> redis.Redis:
    try:
        return redis.Redis.from_url(DEFAULT_REDIS_URL, decode_responses=True)
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=503, detail=f"Redis client init failed: {exc}") from exc


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "service": "straddle-live-api"}


@app.get("/straddle/current/{symbol}")
def get_current(symbol: str) -> dict[str, Any]:
    normalized = _normalize_symbol(symbol)
    client = _get_redis_client()

    try:
        raw = client.get(current_key(normalized))
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=f"Redis error: {exc}") from exc

    if not raw:
        raise HTTPException(status_code=404, detail=f"No current payload for {normalized}")

    try:
        return _to_user_payload(json.loads(raw))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail=f"Invalid JSON payload in Redis: {exc}") from exc


@app.get("/straddle/history/{symbol}")
def get_history(symbol: str, limit: int = Query(default=20, ge=1, le=MAX_HISTORY_LIMIT)) -> list[dict[str, Any]]:
    normalized = _normalize_symbol(symbol)
    client = _get_redis_client()

    start = -limit if limit > 0 else 0
    try:
        rows = client.lrange(history_key(normalized), start, -1)
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=f"Redis error: {exc}") from exc

    parsed: list[dict[str, Any]] = []
    for row in rows:
        try:
            parsed.append(_to_user_payload(json.loads(row)))
        except json.JSONDecodeError:
            continue
    return parsed


@app.get("/straddle/stream/{symbol}")
async def stream_updates(request: Request, symbol: str) -> StreamingResponse:
    normalized = _normalize_symbol(symbol)
    client = _get_redis_client()
    pubsub = client.pubsub(ignore_subscribe_messages=True)

    try:
        pubsub.subscribe(update_channel(normalized))
    except redis.RedisError as exc:
        pubsub.close()
        raise HTTPException(status_code=503, detail=f"Redis subscribe error: {exc}") from exc

    async def event_generator():
        last_heartbeat = time.monotonic()
        try:
            while True:
                if await request.is_disconnected():
                    break

                message = pubsub.get_message(timeout=0.0)
                if message and message.get("type") == "message":
                    raw_payload = str(message.get("data") or "")
                    try:
                        event_payload = _to_user_payload(json.loads(raw_payload))
                    except json.JSONDecodeError:
                        continue
                    yield f"event: update\ndata: {json.dumps(event_payload, ensure_ascii=True)}\n\n"

                now = time.monotonic()
                if now - last_heartbeat >= DEFAULT_STREAM_HEARTBEAT_SECONDS:
                    yield ": keepalive\n\n"
                    last_heartbeat = now

                await asyncio.sleep(0.25)
        finally:
            pubsub.close()

    return StreamingResponse(event_generator(), media_type="text/event-stream")
