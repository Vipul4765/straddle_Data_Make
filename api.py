from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable

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
STREAM_CLIENT_QUEUE_SIZE = 50
STREAM_RECONNECT_BACKOFF_SECONDS = 1.0
STREAM_RECONNECT_MAX_BACKOFF_SECONDS = 5.0


class SharedPubSubManager:
    def __init__(self, redis_url: str, *, queue_maxsize: int = STREAM_CLIENT_QUEUE_SIZE):
        self.redis_url = redis_url
        self.queue_maxsize = queue_maxsize
        self.listeners: dict[str, set[asyncio.Queue[str | None]]] = {}
        self._redis_client: redis.asyncio.Redis | None = None
        self._pubsub = None
        self._listener_task: asyncio.Task | None = None
        self._subscribed_channels: set[str] = set()
        self._subscription_changed = asyncio.Event()
        self._state_lock = asyncio.Lock()
        self._stopping = False

    async def start(self) -> None:
        async with self._state_lock:
            self._stopping = False
            if self._listener_task is None or self._listener_task.done():
                self._listener_task = asyncio.create_task(self._listen_loop())

    async def stop(self) -> None:
        async with self._state_lock:
            self._stopping = True
            self._subscription_changed.set()
            task = self._listener_task
            self._listener_task = None
            self.listeners.clear()

        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        async with self._state_lock:
            await self._close_connection_locked()

    async def subscribe(self, channel: str) -> asyncio.Queue[str | None]:
        queue: asyncio.Queue[str | None] = asyncio.Queue(maxsize=self.queue_maxsize)
        async with self._state_lock:
            self.listeners.setdefault(channel, set()).add(queue)
            self._stopping = False
            if self._listener_task is None or self._listener_task.done():
                self._listener_task = asyncio.create_task(self._listen_loop())
            self._subscription_changed.set()
        return queue

    async def unsubscribe(self, channel: str, queue: asyncio.Queue[str | None]) -> None:
        async with self._state_lock:
            listeners = self.listeners.get(channel)
            if listeners is None or queue not in listeners:
                return
            listeners.remove(queue)
            if not listeners:
                self.listeners.pop(channel, None)
            self._subscription_changed.set()

    async def _ensure_connection_locked(self) -> None:
        if self._redis_client is not None and self._pubsub is not None:
            return
        self._redis_client = redis.asyncio.from_url(self.redis_url, decode_responses=True)
        self._pubsub = self._redis_client.pubsub(ignore_subscribe_messages=True)
        self._subscribed_channels.clear()
        self._subscription_changed.set()

    async def _close_connection_locked(self) -> None:
        pubsub = self._pubsub
        redis_client = self._redis_client
        self._pubsub = None
        self._redis_client = None
        self._subscribed_channels.clear()

        if pubsub is not None:
            try:
                await pubsub.aclose()
            except Exception:
                pass
        if redis_client is not None:
            try:
                await redis_client.aclose()
            except Exception:
                pass

    async def _sync_subscriptions(self) -> None:
        async with self._state_lock:
            pubsub = self._pubsub
            desired_channels = set(self.listeners)
            current_channels = set(self._subscribed_channels)
            self._subscription_changed.clear()

        if pubsub is None:
            return

        to_add = sorted(desired_channels - current_channels)
        to_remove = sorted(current_channels - desired_channels)

        if to_add:
            await pubsub.subscribe(*to_add)
        if to_remove:
            await pubsub.unsubscribe(*to_remove)

        async with self._state_lock:
            if pubsub is not self._pubsub:
                self._subscription_changed.set()
                return
            self._subscribed_channels.update(to_add)
            self._subscribed_channels.difference_update(to_remove)
            if set(self.listeners) != self._subscribed_channels:
                self._subscription_changed.set()

    async def _disconnect_slow_client(self, channel: str, queue: asyncio.Queue[str | None]) -> None:
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        try:
            queue.put_nowait(None)
        except asyncio.QueueFull:
            pass
        await self.unsubscribe(channel, queue)

    async def _dispatch_message(self, channel: str, data: str) -> None:
        async with self._state_lock:
            listeners = list(self.listeners.get(channel, ()))

        slow_clients: list[asyncio.Queue[str | None]] = []
        for queue in listeners:
            try:
                queue.put_nowait(data)
            except asyncio.QueueFull:
                slow_clients.append(queue)

        for queue in slow_clients:
            await self._disconnect_slow_client(channel, queue)

    async def _listen_loop(self) -> None:
        backoff = STREAM_RECONNECT_BACKOFF_SECONDS
        try:
            while True:
                async with self._state_lock:
                    if self._stopping:
                        return
                    await self._ensure_connection_locked()
                    pubsub = self._pubsub

                try:
                    while True:
                        if self._stopping:
                            return
                        if self._subscription_changed.is_set():
                            await self._sync_subscriptions()

                        if pubsub is None:
                            break

                        if not self._subscribed_channels:
                            try:
                                await asyncio.wait_for(self._subscription_changed.wait(), timeout=1.0)
                            except asyncio.TimeoutError:
                                pass
                            continue

                        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                        if not message or message.get("type") != "message":
                            continue

                        channel_name = _decode_pubsub_data(message.get("channel", ""))
                        data = _decode_pubsub_data(message.get("data", ""))
                        await self._dispatch_message(channel_name, data)

                    backoff = STREAM_RECONNECT_BACKOFF_SECONDS
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    print(f"SharedPubSubManager listener error: {exc}")
                    async with self._state_lock:
                        await self._close_connection_locked()

                    if self._stopping:
                        return

                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, STREAM_RECONNECT_MAX_BACKOFF_SECONDS)
        except asyncio.CancelledError:
            pass
        finally:
            async with self._state_lock:
                await self._close_connection_locked()


broadcaster = SharedPubSubManager(REDIS_URL)


@asynccontextmanager
async def lifespan(application: FastAPI):
    _ = application
    await broadcaster.start()
    yield
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


def _decode_pubsub_data(raw: Any) -> str:
    if isinstance(raw, (bytes, bytearray)):
        return raw.decode("utf-8", errors="replace")
    return str(raw)


def _load_straddle_snapshot(symbol: str) -> str | None:
    client = _redis_client()
    try:
        raw = client.get(current_key(symbol))
    finally:
        client.close()
    return raw or None


def _load_index_snapshot(symbol: str) -> str | None:
    token = RAW_INDEX_SYMBOLS[symbol]["pubsub_token"]
    client = _redis_client()
    try:
        raw = client.hget(source_hash_key(token), "__latest__")
    finally:
        client.close()
    payload = _parse_latest_hash_payload(raw, token, symbol)
    if payload is None:
        return None
    return json.dumps(payload, ensure_ascii=True)


async def _stream_shared_channel(
    channel: str,
    formatter: Callable[[str], str | None],
    snapshot_loader: Callable[[], str | None] | None = None,
) -> AsyncIterator[str]:
    queue = await broadcaster.subscribe(channel)
    heartbeat_every = float(STRADDLE_STREAM_HEARTBEAT_SECONDS)
    last_heartbeat = time.monotonic()
    last_sent: str | None = None

    try:
        yield ": keepalive\n\n"

        if snapshot_loader is not None:
            snapshot = await asyncio.to_thread(snapshot_loader)
            if snapshot is not None:
                yield f"event: update\ndata: {snapshot}\n\n"
                last_sent = snapshot
                last_heartbeat = time.monotonic()

        while True:
            time_to_wait = max(0.0, heartbeat_every - (time.monotonic() - last_heartbeat))
            try:
                raw_data = await asyncio.wait_for(queue.get(), timeout=min(1.0, time_to_wait))
            except asyncio.TimeoutError:
                raw_data = "__timeout__"

            if raw_data is None:
                break

            if raw_data != "__timeout__":
                try:
                    data = formatter(raw_data)
                except Exception as exc:  # noqa: BLE001
                    print(f"SSE formatter error channel={channel}: {exc}")
                    data = None

                if data is not None and data != last_sent:
                    yield f"event: update\ndata: {data}\n\n"
                    last_sent = data
                    last_heartbeat = time.monotonic()
                    continue

            now = time.monotonic()
            if now - last_heartbeat >= heartbeat_every:
                yield ": keepalive\n\n"
                last_heartbeat = now
    except asyncio.CancelledError:
        pass
    except Exception as exc:  # noqa: BLE001
        print(f"SSE stream error channel={channel}: {exc}")
    finally:
        try:
            await asyncio.shield(broadcaster.unsubscribe(channel, queue))
        except Exception:
            pass


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

    return StreamingResponse(
        _stream_shared_channel(
            update_channel(symbol),
            lambda data: data,
            snapshot_loader=lambda: _load_straddle_snapshot(symbol),
        ),
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

    return StreamingResponse(
        _stream_shared_channel(
            REDIS_PUBSUB_CHANNEL,
            lambda data: (
                json.dumps(parsed, ensure_ascii=True)
                if (parsed := _normalize_index_pubsub_event(data, symbol, token)) is not None
                else None
            ),
            snapshot_loader=lambda: _load_index_snapshot(symbol),
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
