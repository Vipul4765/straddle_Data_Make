from __future__ import annotations


def current_key(symbol: str) -> str:
    return f"straddle:current:{symbol.upper()}"


def update_channel(symbol: str) -> str:
    return f"straddle:update:{symbol.upper()}"


def version_key(symbol: str) -> str:
    return f"straddle:version:{symbol.upper()}"


def history_key(symbol: str) -> str:
    return f"straddle:history:{symbol.upper()}"


def source_hash_key(pubsub_spot_token: str) -> str:
    return f"ohlc:1m:{pubsub_spot_token}"
