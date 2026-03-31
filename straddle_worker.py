from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import time
from typing import Any

import redis

try:
    from .contract_data import SYMBOL_CONFIG, contract_file
    from .redis_keys import (
        current_key,
        daily_reset_marker_key,
        history_key,
        source_hash_key,
        update_channel,
        version_key,
    )
    from .settings import (
        REDIS_PUBSUB_CHANNEL,
        REDIS_URL,
        STRADDLE_MAX_HISTORY,
        STRADDLE_POLL_INTERVAL_SECONDS,
        STRADDLE_STARTUP_BACKFILL_CANDLES,
        STRADDLE_SYMBOLS,
        TRADING_CANDLE_END,
        TRADING_CANDLE_START,
        TRADING_HOLIDAYS,
        TRADING_RESET_TIME,
        TRADING_SESSION_END,
        TRADING_SESSION_START,
        DATABASE_URL,
        MARKET_HOLIDAY_EXCHANGES,
    )
    from .straddle_builder import atm_strike, build_straddle_for_strike
    from trading_calendar import TradingCalendar, parse_holidays, parse_time
except ImportError:  # Support direct script execution
    from contract_data import SYMBOL_CONFIG, contract_file
    from redis_keys import (
        current_key,
        daily_reset_marker_key,
        history_key,
        source_hash_key,
        update_channel,
        version_key,
    )
    from settings import (
        REDIS_PUBSUB_CHANNEL,
        REDIS_URL,
        STRADDLE_MAX_HISTORY,
        STRADDLE_POLL_INTERVAL_SECONDS,
        STRADDLE_STARTUP_BACKFILL_CANDLES,
        STRADDLE_SYMBOLS,
        TRADING_CANDLE_END,
        TRADING_CANDLE_START,
        TRADING_HOLIDAYS,
        TRADING_RESET_TIME,
        TRADING_SESSION_END,
        TRADING_SESSION_START,
        DATABASE_URL,
        MARKET_HOLIDAY_EXCHANGES,
    )
    from straddle_builder import atm_strike, build_straddle_for_strike
    from trading_calendar import TradingCalendar, parse_holidays, parse_time


def _now_ms() -> int:
    return int(time.time() * 1000)


def _minute_str(minute_int: int) -> str:
    return f"{minute_int // 10000:02d}:{(minute_int // 100) % 100:02d}:00"


def _previous_minute_int(minute_int: int) -> int:
    hour = minute_int // 10000
    minute = (minute_int // 100) % 100
    minute -= 1
    if minute >= 0:
        return hour * 10000 + minute * 100
    return (hour - 1) * 10000 + 5900


def _symbol_from_pubsub_token(token: str) -> str | None:
    for symbol, config in SYMBOL_CONFIG.items():
        if token == config["pubsub_spot_token"]:
            return symbol
    return None


def _parse_latest_payload(raw: str | None, token: str) -> dict[str, Any] | None:
    if not raw:
        return None
    values = json.loads(raw)
    if not isinstance(values, list) or len(values) < 6:
        return None
    minute_int = int(values[5])
    return {
        "token": token,
        "minute_int": minute_int,
        "minute": minute_int,
        "minute_str": _minute_str(minute_int),
        "open": float(values[0]),
        "high": float(values[1]),
        "low": float(values[2]),
        "close": float(values[3]),
        "volume": float(values[4]),
        "closed": True,
    }


def _parse_history_payload(raw: str | None, token: str, minute_int: int) -> dict[str, Any] | None:
    if not raw:
        return None
    values = json.loads(raw)
    if not isinstance(values, list) or len(values) < 4:
        return None
    volume = float(values[4]) if len(values) > 4 else 0.0
    return {
        "token": token,
        "minute_int": minute_int,
        "minute": minute_int,
        "minute_str": _minute_str(minute_int),
        "open": float(values[0]),
        "high": float(values[1]),
        "low": float(values[2]),
        "close": float(values[3]),
        "volume": volume,
        "closed": True,
    }


def _normalize_event(raw: str) -> dict[str, Any]:
    event = json.loads(raw)
    event["token"] = str(event.get("token") or "")
    event["minute_int"] = int(event.get("minute") or event.get("minute_int") or 0)
    event["minute"] = event["minute_int"]
    event["open"] = float(event["open"])
    event["high"] = float(event["high"])
    event["low"] = float(event["low"])
    event["close"] = float(event["close"])
    event["volume"] = float(event.get("volume") or 0)
    event["minute_str"] = str(event.get("minute_str") or "")
    return event


def _mask_secret_url(raw_url: str) -> str:
    return re.sub(r"(://[^:/?#]*:)[^@/]+@", r"\1***@", raw_url)


CALENDAR = TradingCalendar(
    session_start=parse_time(TRADING_SESSION_START, field_name="TRADING_SESSION_START"),
    session_end=parse_time(TRADING_SESSION_END, field_name="TRADING_SESSION_END"),
    reset_time=parse_time(TRADING_RESET_TIME, field_name="TRADING_RESET_TIME"),
    candle_start=parse_time(TRADING_CANDLE_START, field_name="TRADING_CANDLE_START"),
    candle_end=parse_time(TRADING_CANDLE_END, field_name="TRADING_CANDLE_END"),
    holidays=parse_holidays(TRADING_HOLIDAYS),
    database_url=DATABASE_URL,
    holiday_exchanges=MARKET_HOLIDAY_EXCHANGES,
)
CANDLE_START_MINUTE_INT, CANDLE_END_MINUTE_INT = CALENDAR.candle_minute_bounds()


def _ist_now() -> dt.datetime:
    return CALENDAR.now()


def _reset_due_now(now_ist: dt.datetime) -> bool:
    return CALENDAR.reset_is_due(now_ist)


def _minute_within_market_session(minute_int: int) -> bool:
    return CANDLE_START_MINUTE_INT <= minute_int <= CANDLE_END_MINUTE_INT


def _within_processing_grace(now_ist: dt.datetime) -> bool:
    return CALENDAR.session_is_open(now_ist)


def _current_session_max_minute(now_ist: dt.datetime) -> int:
    return CALENDAR.current_session_max_minute(now_ist)


class SharedStraddleWorker:
    def __init__(
        self,
        redis_url: str,
        source_channel: str,
        symbols: set[str],
        poll_interval_seconds: float,
        max_history: int,
        startup_backfill_candles: int,
    ) -> None:
        self.redis_url = redis_url
        self.source_channel = source_channel
        self.symbols = symbols
        self.poll_interval_seconds = poll_interval_seconds
        self.max_history = max_history
        self.startup_backfill_candles = max(0, startup_backfill_candles)
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self.segment_wise_contract_dict, _ = contract_file()
        self.latest_event_by_token: dict[str, dict[str, Any]] = {}
        self.recent_events_by_token: dict[str, dict[int, dict[str, Any]]] = {}
        self.last_published: dict[str, tuple[int, float, str, str]] = {}
        self.tracked_tokens = self._build_tracked_tokens()
        self.spot_tokens = {SYMBOL_CONFIG[symbol]["pubsub_spot_token"] for symbol in self.symbols}
        self.wait_for_spot_after_reset: set[str] = set()
        self.contracts_loaded_for_date: str | None = None

    def _build_tracked_tokens(self) -> set[str]:
        tracked: set[str] = set()
        for symbol in self.symbols:
            config = SYMBOL_CONFIG[symbol]
            tracked.add(config["pubsub_spot_token"])
            fo_tree = self.segment_wise_contract_dict[config["fo_segment"]][symbol]
            for option_type in ("CE", "PE"):
                for row in fo_tree[option_type][1].values():
                    tracked.add(f"{config['fo_segment']}:{row['exchange_token']}")
        return tracked

    def run(self) -> None:
        print(f"redis={_mask_secret_url(self.redis_url)}")
        print(f"source_channel={self.source_channel}")
        print(f"symbols={','.join(sorted(self.symbols))}")
        self._clear_legacy_current_payloads()
        self._run_daily_reset_if_due()
        if _within_processing_grace(_ist_now()):
            self._bootstrap_from_hashes()

        pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(self.source_channel)
        next_poll_at = 0.0

        try:
            while True:
                now_ist = _ist_now()
                if not _within_processing_grace(now_ist):
                    self._run_daily_reset_if_due()
                    time.sleep(min(max(self.poll_interval_seconds, 1.0), CALENDAR.seconds_until_next_session(now_ist) or 1.0))
                    continue

                message = pubsub.get_message(timeout=1.0)
                if message and message["type"] == "message":
                    self._handle_pubsub_message(message["data"])

                now = time.monotonic()
                if now >= next_poll_at:
                    self._run_daily_reset_if_due()
                    if _within_processing_grace(_ist_now()):
                        self._refresh_from_hashes()
                    next_poll_at = now + self.poll_interval_seconds
        finally:
            pubsub.close()

    def _run_daily_reset_if_due(self) -> None:
        now_ist = _ist_now()
        if not _reset_due_now(now_ist):
            return

        today = now_ist.date().isoformat()
        self._refresh_contracts_for_trading_day(today)
        for symbol in sorted(self.symbols):
            marker_key = daily_reset_marker_key(symbol)
            already_reset_for_today = self.redis.get(marker_key) == today
            if already_reset_for_today:
                continue

            pipe = self.redis.pipeline(transaction=False)
            pipe.delete(current_key(symbol))
            pipe.delete(history_key(symbol))
            pipe.set(marker_key, today)
            pipe.execute()

            self.last_published.pop(symbol, None)
            self.wait_for_spot_after_reset.add(symbol)
            print(f"daily_reset symbol={symbol} date={today} at_or_after={TRADING_RESET_TIME}IST")

    def _refresh_contracts_for_trading_day(self, trading_day: str) -> None:
        if self.contracts_loaded_for_date == trading_day:
            return

        self.segment_wise_contract_dict, _ = contract_file()
        self.tracked_tokens = self._build_tracked_tokens()
        self.spot_tokens = {SYMBOL_CONFIG[symbol]["pubsub_spot_token"] for symbol in self.symbols}
        self.latest_event_by_token.clear()
        self.recent_events_by_token.clear()
        self.contracts_loaded_for_date = trading_day
        print(f"contracts_refreshed date={trading_day}")

    def _bootstrap_from_hashes(self) -> None:
        bootstrap_events = self._fetch_latest_hash_events(self.tracked_tokens)
        for event in bootstrap_events.values():
            self._remember_event(event)

        for symbol in sorted(self.symbols):
            backfilled = self._bootstrap_history_for_symbol(symbol)
            if backfilled <= 0:
                self._maybe_publish_for_symbol(symbol, source="bootstrap")

    def _bootstrap_history_for_symbol(self, symbol: str) -> int:
        minute_values = self._recent_minutes_for_symbol(symbol, self.startup_backfill_candles)
        if not minute_values:
            return 0

        encoded_payloads: list[str] = []
        last_identity: tuple[int, float, str, str] | None = None
        for minute_int in minute_values:
            built = self._build_payload_for_symbol_minute(symbol, minute_int, source="startup_backfill")
            if not built:
                continue

            payload, identity = built
            payload["version"] = self.redis.incr(version_key(symbol))
            encoded_payloads.append(json.dumps(payload, ensure_ascii=True))
            last_identity = identity

        if not encoded_payloads or not last_identity:
            return 0

        pipe = self.redis.pipeline(transaction=False)
        pipe.delete(history_key(symbol))
        pipe.rpush(history_key(symbol), *encoded_payloads)
        pipe.set(current_key(symbol), encoded_payloads[-1])
        pipe.execute()

        self.last_published[symbol] = last_identity
        print(
            f"startup_backfill symbol={symbol} candles={len(encoded_payloads)} "
            f"from={minute_values[0]} to={minute_values[-1]}"
        )
        return len(encoded_payloads)

    def _recent_minutes_for_symbol(self, symbol: str, limit: int) -> list[int]:
        spot_token = SYMBOL_CONFIG[symbol]["pubsub_spot_token"]
        try:
            raw_fields = self.redis.hkeys(source_hash_key(spot_token))
        except redis.RedisError:
            return []

        minute_values: list[int] = []
        for raw_field in raw_fields:
            if raw_field == "__latest__":
                continue
            try:
                minute_value = int(raw_field)
            except (TypeError, ValueError):
                continue
            if minute_value > 0:
                minute_values.append(minute_value)

        if not minute_values:
            return []
        unique_sorted_minutes = sorted(set(minute_values))
        session_minutes = [
            minute
            for minute in unique_sorted_minutes
            if _minute_within_market_session(minute) and minute <= _current_session_max_minute(_ist_now())
        ]
        if not session_minutes:
            return []
        if limit <= 0:
            return session_minutes
        return session_minutes[-limit:]

    def _build_payload_for_symbol_minute(
        self,
        symbol: str,
        minute_int: int,
        source: str,
    ) -> tuple[dict[str, Any], tuple[int, float, str, str]] | None:
        config = SYMBOL_CONFIG[symbol]
        spot_event = self._load_leg_event(config["pubsub_spot_token"], minute_int)
        if not spot_event:
            return None

        selected = self._select_exact_atm_strike(symbol, spot_event["close"], minute_int)
        if not selected:
            return None
        requested_atm_strike, selection, ce_event, pe_event = selected

        ce_pubsub_token = f"{selection.fo_segment}:{selection.ce_exchange_token}"
        pe_pubsub_token = f"{selection.fo_segment}:{selection.pe_exchange_token}"

        identity = (
            minute_int,
            selection.strike,
            selection.ce_exchange_token,
            selection.pe_exchange_token,
        )
        ce_carry_forward = ce_event["minute_int"] != minute_int
        pe_carry_forward = pe_event["minute_int"] != minute_int
        payload = {
            "symbol": symbol,
            "fo_segment": selection.fo_segment,
            "expiry_rank": 1,
            "minute_int": minute_int,
            "minute_str": spot_event["minute_str"] or ce_event["minute_str"] or pe_event["minute_str"],
            "spot_price": float(spot_event["close"]),
            "spot_pubsub_token": config["pubsub_spot_token"],
            "atm_strike": float(requested_atm_strike),
            "strike": float(selection.strike),
            "selected_from_atm": False,
            "ce_instrument_key": selection.ce_instrument_key,
            "pe_instrument_key": selection.pe_instrument_key,
            "ce_exchange_token": selection.ce_exchange_token,
            "pe_exchange_token": selection.pe_exchange_token,
            "ce_pubsub_token": ce_pubsub_token,
            "pe_pubsub_token": pe_pubsub_token,
            "ce_carry_forward": ce_carry_forward,
            "pe_carry_forward": pe_carry_forward,
            "carry_forward": ce_carry_forward or pe_carry_forward,
            "ce_open": float(ce_event["open"]),
            "ce_high": float(ce_event["high"]),
            "ce_low": float(ce_event["low"]),
            "ce_close": float(ce_event["close"]),
            "ce_volume": float(ce_event.get("volume") or 0),
            "pe_open": float(pe_event["open"]),
            "pe_high": float(pe_event["high"]),
            "pe_low": float(pe_event["low"]),
            "pe_close": float(pe_event["close"]),
            "pe_volume": float(pe_event.get("volume") or 0),
            "open": float(ce_event["open"]) + float(pe_event["open"]),
            "high": float(ce_event["high"]) + float(pe_event["high"]),
            "low": float(ce_event["low"]) + float(pe_event["low"]),
            "close": float(ce_event["close"]) + float(pe_event["close"]),
            "straddle_price": float(ce_event["close"]) + float(pe_event["close"]),
            "volume": float(ce_event.get("volume") or 0) + float(pe_event.get("volume") or 0),
            "source": source,
            "updated_at_ms": _now_ms(),
        }
        return payload, identity

    def _fetch_latest_hash_events(self, tokens: set[str]) -> dict[str, dict[str, Any]]:
        if not tokens:
            return {}

        ordered_tokens = sorted(tokens)
        pipe = self.redis.pipeline(transaction=False)
        for token in ordered_tokens:
            pipe.hget(source_hash_key(token), "__latest__")

        rows = pipe.execute()
        parsed_events: dict[str, dict[str, Any]] = {}
        for token, raw in zip(ordered_tokens, rows):
            parsed = _parse_latest_payload(raw, token)
            if parsed:
                parsed_events[token] = parsed
        return parsed_events

    def _remember_event(self, event: dict[str, Any]) -> None:
        token = str(event["token"])
        minute_int = int(event["minute_int"])
        self.latest_event_by_token[token] = event
        token_events = self.recent_events_by_token.setdefault(token, {})
        token_events[minute_int] = event
        if len(token_events) > RECENT_EVENT_CACHE_SIZE:
            for old_minute in sorted(token_events)[:-RECENT_EVENT_CACHE_SIZE]:
                token_events.pop(old_minute, None)

    def _clear_legacy_current_payloads(self) -> None:
        for symbol in sorted(self.symbols):
            raw = self.redis.get(current_key(symbol))
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                self.redis.delete(current_key(symbol))
                continue
            if "minute_int" not in payload or "open" not in payload or "close" not in payload:
                self.redis.delete(current_key(symbol))

    def _handle_pubsub_message(self, raw: str) -> None:
        if not _within_processing_grace(_ist_now()):
            return
        event = _normalize_event(raw)
        token = event["token"]
        if token not in self.tracked_tokens:
            return
        self._remember_event(event)

        symbol = _symbol_from_pubsub_token(token)
        if symbol and symbol in self.symbols:
            # After the daily reset, require one fresh spot tick before republishing.
            self.wait_for_spot_after_reset.discard(symbol)
            self._maybe_publish_for_symbol(symbol, source="pubsub_spot")
            return

        for candidate_symbol in self.symbols:
            config = SYMBOL_CONFIG[candidate_symbol]
            if token.startswith(f"{config['fo_segment']}:"):
                self._maybe_publish_for_symbol(candidate_symbol, source="pubsub")

    def _refresh_from_hashes(self) -> None:
        for token, parsed in self._fetch_latest_hash_events(self.spot_tokens).items():
            if not parsed:
                continue
            self._remember_event(parsed)

        for symbol in sorted(self.symbols):
            self._maybe_publish_for_symbol(symbol, source="redis_hash")

    def _maybe_publish_for_symbol(self, symbol: str, source: str) -> None:
        if symbol in self.wait_for_spot_after_reset and source != "pubsub_spot":
            return

        config = SYMBOL_CONFIG[symbol]
        spot_event = self.latest_event_by_token.get(config["pubsub_spot_token"])
        if not spot_event:
            return

        minute_int = int(spot_event["minute_int"])
        if not _minute_within_market_session(minute_int):
            return

        previous_identity = self.last_published.get(symbol)
        if previous_identity and minute_int <= previous_identity[0]:
            return

        built = self._build_payload_for_symbol_minute(symbol, minute_int, source)
        if not built:
            return
        payload, identity = built

        version = self.redis.incr(version_key(symbol))
        payload["version"] = version

        encoded = json.dumps(payload, ensure_ascii=True)
        last_history_raw = self.redis.lindex(history_key(symbol), -1)
        replace_history_tail = False
        if last_history_raw:
            try:
                last_history = json.loads(last_history_raw)
                replace_history_tail = int(last_history.get("minute_int", -1)) == minute_int
            except json.JSONDecodeError:
                replace_history_tail = False
        pipe = self.redis.pipeline(transaction=False)
        pipe.set(current_key(symbol), encoded)
        if replace_history_tail:
            pipe.lset(history_key(symbol), -1, encoded)
        else:
            pipe.rpush(history_key(symbol), encoded)
            pipe.ltrim(history_key(symbol), -self.max_history, -1)
        pipe.publish(update_channel(symbol), encoded)
        pipe.execute()

        self.last_published[symbol] = identity
        print(
            f"published symbol={symbol} minute={minute_int} strike={payload['strike']} "
            f"close={payload['close']} source={source}"
        )

    def _select_exact_atm_strike(
        self,
        symbol: str,
        spot_price: float,
        minute_int: int,
    ) -> tuple[float, Any, dict[str, Any], dict[str, Any]] | None:
        config = SYMBOL_CONFIG[symbol]
        requested_atm_strike = atm_strike(symbol, spot_price)
        try:
            selection = build_straddle_for_strike(
                self.segment_wise_contract_dict,
                symbol,
                spot_price,
                float(requested_atm_strike),
            )
        except KeyError:
            return None

        ce_event = self._load_leg_event(
            token=f"{config['fo_segment']}:{selection.ce_exchange_token}",
            minute_int=minute_int,
        )
        pe_event = self._load_leg_event(
            token=f"{config['fo_segment']}:{selection.pe_exchange_token}",
            minute_int=minute_int,
        )
        if not ce_event or not pe_event:
            return None
        return requested_atm_strike, selection, ce_event, pe_event

    def _load_leg_event(self, token: str, minute_int: int) -> dict[str, Any] | None:
        cached_exact = self.recent_events_by_token.get(token, {}).get(minute_int)
        if cached_exact:
            return cached_exact

        exact_raw = self.redis.hget(source_hash_key(token), str(minute_int))
        exact_parsed = _parse_history_payload(exact_raw, token, minute_int)
        if exact_parsed:
            self._remember_event(exact_parsed)
            return exact_parsed

        cached = self.latest_event_by_token.get(token)
        if cached and cached["minute_int"] == minute_int:
            return cached

        parsed = _parse_latest_payload(self.redis.hget(source_hash_key(token), "__latest__"), token)
        if parsed:
            self._remember_event(parsed)
            if parsed["minute_int"] == minute_int:
                return parsed

        older_cached = self._latest_cached_at_or_before(token, minute_int)
        if older_cached:
            return self._forward_fill_event(older_cached, token, minute_int)

        older_from_hash = self._latest_hash_event_at_or_before(token, minute_int)
        if older_from_hash:
            self._remember_event(older_from_hash)
            return self._forward_fill_event(older_from_hash, token, minute_int)

        latest = self.latest_event_by_token.get(token)
        if not latest or latest["minute_int"] > minute_int:
            return None
        return self._forward_fill_event(latest, token, minute_int)

    def _latest_cached_at_or_before(self, token: str, minute_int: int) -> dict[str, Any] | None:
        token_events = self.recent_events_by_token.get(token, {})
        eligible = [candidate_minute for candidate_minute in token_events if candidate_minute <= minute_int]
        if not eligible:
            return None
        return token_events[max(eligible)]

    def _latest_hash_event_at_or_before(self, token: str, minute_int: int, lookback: int = 10) -> dict[str, Any] | None:
        probe_minute = minute_int
        for _ in range(lookback):
            probe_minute = _previous_minute_int(probe_minute)
            if probe_minute <= 0:
                break
            raw = self.redis.hget(source_hash_key(token), str(probe_minute))
            parsed = _parse_history_payload(raw, token, probe_minute)
            if parsed:
                return parsed
        return None

    def _forward_fill_event(self, event: dict[str, Any], token: str, minute_int: int) -> dict[str, Any]:
        close_price = float(event["close"])
        return {
            "token": token,
            "minute_int": minute_int,
            "minute": minute_int,
            "minute_str": _minute_str(minute_int),
            "open": close_price,
            "high": close_price,
            "low": close_price,
            "close": close_price,
            "volume": 0.0,
            "closed": True,
        }

DEFAULT_REDIS_URL = REDIS_URL
DEFAULT_SOURCE_CHANNEL = REDIS_PUBSUB_CHANNEL
DEFAULT_SYMBOLS = STRADDLE_SYMBOLS
DEFAULT_POLL_INTERVAL_SECONDS = STRADDLE_POLL_INTERVAL_SECONDS
DEFAULT_MAX_HISTORY = STRADDLE_MAX_HISTORY
DEFAULT_STARTUP_BACKFILL_CANDLES = STRADDLE_STARTUP_BACKFILL_CANDLES
RECENT_EVENT_CACHE_SIZE = 8


def main() -> None:
    parser = argparse.ArgumentParser(description="Shared Redis-backed straddle worker")
    parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    parser.add_argument("--source-channel", default=DEFAULT_SOURCE_CHANNEL)
    parser.add_argument("--poll-interval-seconds", type=float, default=DEFAULT_POLL_INTERVAL_SECONDS)
    parser.add_argument("--max-history", type=int, default=DEFAULT_MAX_HISTORY)
    parser.add_argument("--startup-backfill-candles", type=int, default=DEFAULT_STARTUP_BACKFILL_CANDLES)
    parser.add_argument("--symbols", default=DEFAULT_SYMBOLS)
    args = parser.parse_args()

    symbols = {item.strip().upper() for item in args.symbols.split(",") if item.strip()}
    SharedStraddleWorker(
        redis_url=args.redis_url,
        source_channel=args.source_channel,
        symbols=symbols,
        poll_interval_seconds=args.poll_interval_seconds,
        max_history=args.max_history,
        startup_backfill_candles=args.startup_backfill_candles,
    ).run()


if __name__ == "__main__":
    main()