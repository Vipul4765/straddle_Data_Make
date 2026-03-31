from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

try:
    from .env_loader import load_local_env
except ImportError:  # Support direct script execution
    from env_loader import load_local_env

load_local_env()

PACKAGE_DIR = Path(__file__).resolve().parent


def _resolve_path(raw_value: str | None, default_path: Path) -> Path:
    if not raw_value:
        return default_path
    candidate = Path(raw_value).expanduser()
    if candidate.is_absolute():
        return candidate
    return PACKAGE_DIR / candidate


def _env_str(name: str, default: str | None = None) -> str:
    value = os.getenv(name)
    if value not in {None, ""}:
        return value
    if default is not None:
        return default
    raise RuntimeError(f"Missing required environment variable: {name}")


def _env_int(name: str, default: int | None = None) -> int:
    return int(_env_str(name, None if default is None else str(default)))


def _env_float(name: str, default: float | None = None) -> float:
    return float(_env_str(name, None if default is None else str(default)))


def _load_symbol_config(path: Path) -> dict[str, dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Symbol config at {path} must be a JSON object")

    required_fields = {
        "fo_segment",
        "spot_segment",
        "spot_name",
        "spot_trading_symbol",
        "pubsub_spot_token",
        "strike_step",
    }
    normalized: dict[str, dict[str, Any]] = {}
    for raw_symbol, raw_config in payload.items():
        if not isinstance(raw_config, dict):
            raise ValueError(f"Config for symbol {raw_symbol!r} must be a JSON object")

        symbol = str(raw_symbol).strip().upper()
        missing_fields = sorted(required_fields.difference(raw_config))
        if missing_fields:
            raise ValueError(f"Config for symbol {symbol!r} is missing: {', '.join(missing_fields)}")

        normalized[symbol] = {
            "fo_segment": str(raw_config["fo_segment"]),
            "spot_segment": str(raw_config["spot_segment"]),
            "spot_name": str(raw_config["spot_name"]),
            "spot_trading_symbol": str(raw_config["spot_trading_symbol"]),
            "pubsub_spot_token": str(raw_config["pubsub_spot_token"]),
            "strike_step": int(raw_config["strike_step"]),
        }

    if not normalized:
        raise ValueError(f"Symbol config at {path} is empty")
    return normalized


SYMBOL_CONFIG_FILE = _resolve_path(
    os.getenv("STRADDLE_SYMBOL_CONFIG_FILE"),
    PACKAGE_DIR / "symbol_config.json",
)
SYMBOL_CONFIG = _load_symbol_config(SYMBOL_CONFIG_FILE)

REDIS_URL = _env_str("REDIS_URL")
REDIS_PUBSUB_CHANNEL = _env_str("REDIS_PUBSUB_CHANNEL")
STRADDLE_SYMBOLS = _env_str("STRADDLE_SYMBOLS", ",".join(SYMBOL_CONFIG))
STRADDLE_POLL_INTERVAL_SECONDS = _env_float("STRADDLE_POLL_INTERVAL_SECONDS")
STRADDLE_MAX_HISTORY = _env_int("STRADDLE_MAX_HISTORY")
STRADDLE_STARTUP_BACKFILL_CANDLES = _env_int("STRADDLE_STARTUP_BACKFILL_CANDLES", 0)
TRADING_SESSION_START = _env_str("TRADING_SESSION_START", "09:15:00")
TRADING_SESSION_END = _env_str("TRADING_SESSION_END", "15:30:00")
TRADING_RESET_TIME = _env_str("TRADING_RESET_TIME", "09:14:00")
TRADING_CANDLE_START = _env_str("TRADING_CANDLE_START", "09:15:00")
TRADING_CANDLE_END = _env_str("TRADING_CANDLE_END", "15:30:00")
TRADING_HOLIDAYS = _env_str("TRADING_HOLIDAYS", "")
STRADDLE_STREAM_HEARTBEAT_SECONDS = _env_float("STRADDLE_STREAM_HEARTBEAT_SECONDS")
STRADDLE_API_MAX_HISTORY_LIMIT = _env_int("STRADDLE_API_MAX_HISTORY_LIMIT")
STRADDLE_CONTRACT_URL = _env_str("STRADDLE_CONTRACT_URL")
STRADDLE_CONTRACT_OUTPUT_FILE = _resolve_path(
    os.getenv("STRADDLE_CONTRACT_OUTPUT_FILE"),
    PACKAGE_DIR / "pub_sub_tokens.txt",
)
STRADDLE_CONTRACT_TIMEOUT = _env_int("STRADDLE_CONTRACT_TIMEOUT")

DATABASE_URL = os.getenv("DATABASE_URL")
MARKET_HOLIDAY_EXCHANGES = _env_str("MARKET_HOLIDAY_EXCHANGES", "NSE,BSE")