from __future__ import annotations

import datetime as dt
import re

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))


def parse_time(raw_value: str, *, field_name: str) -> dt.time:
    try:
        return dt.time.fromisoformat(raw_value)
    except ValueError as exc:
        raise ValueError(f"Invalid time for {field_name}: {raw_value!r}. Expected HH:MM[:SS].") from exc


def parse_holidays(raw_value: str) -> set[dt.date]:
    holidays: set[dt.date] = set()
    for part in raw_value.split(","):
        candidate = part.strip()
        if not candidate:
            continue
        try:
            holidays.add(dt.date.fromisoformat(candidate))
        except ValueError as exc:
            raise ValueError(
                f"Invalid holiday date {candidate!r}. Expected ISO format YYYY-MM-DD."
            ) from exc
    return holidays


class TradingCalendar:
    def __init__(
        self,
        *,
        session_start: dt.time,
        session_end: dt.time,
        reset_time: dt.time,
        candle_start: dt.time,
        candle_end: dt.time,
        holidays: set[dt.date],
        database_url: str | None = None,
        holiday_exchanges: str = "NSE,BSE",
    ) -> None:
        self.session_start = session_start
        self.session_end = session_end
        self.reset_time = reset_time
        self.candle_start = candle_start
        self.candle_end = candle_end
        self.holidays = holidays
        self.holiday_exchanges = {
            item.strip().upper() for item in re.split(r"[,\s;|]+", holiday_exchanges) if item.strip()
        }
        self.database_url = database_url.strip() if database_url else None
        self._db_engine = None
        if self.database_url:
            try:
                self._db_engine = create_engine(self.database_url, pool_pre_ping=True, pool_recycle=3600)
            except Exception as e:
                print(f"ERROR: Failed to create db engine: {e}")
                
        self._db_holidays_by_date: dict[dt.date, bool] = {}

    def now(self) -> dt.datetime:
        return dt.datetime.now(tz=IST)

    def _is_db_holiday(self, target_date: dt.date) -> bool:
        if not self._db_engine:
            return False
            
        cached = self._db_holidays_by_date.get(target_date)
        if cached is not None:
            return cached

        try:
            with self._db_engine.connect() as conn:
                result = conn.execute(
                    text("SELECT exchange FROM holidays WHERE date = :target_date"),
                    {"target_date": target_date},
                )
                exchanges = {str(row[0]).upper() for row in result if row and row[0]}
                
            is_holiday = bool(exchanges.intersection(self.holiday_exchanges))
            self._db_holidays_by_date[target_date] = is_holiday
            return is_holiday
            
        except SQLAlchemyError as exc:
            print(f"ERROR: Treating {target_date} as non-trading temporarily because holidays table lookup failed: {exc}")
            # Do NOT cache the failure so it can auto-recover if DB comes back online
            return True

    def is_trading_day(self, target_date: dt.date) -> bool:
        if target_date in self.holidays:
            return False
        if self._db_engine:
            return not self._is_db_holiday(target_date)
        return True

    def session_is_open(self, now_ist: dt.datetime) -> bool:
        return self.is_trading_day(now_ist.date()) and self.session_start <= now_ist.time() <= self.session_end

    def reset_is_due(self, now_ist: dt.datetime) -> bool:
        return self.is_trading_day(now_ist.date()) and now_ist.time() >= self.reset_time

    def candle_minute_bounds(self) -> tuple[int, int]:
        return self._time_to_minute_int(self.candle_start), self._time_to_minute_int(self.candle_end)

    def current_session_max_minute(self, now_ist: dt.datetime) -> int:
        candle_start_minute, candle_end_minute = self.candle_minute_bounds()
        minute_int = self._time_to_minute_int(now_ist.time().replace(second=0, microsecond=0))
        if minute_int < candle_start_minute:
            return candle_start_minute
        return min(minute_int, candle_end_minute)

    def seconds_until_next_session(self, now_ist: dt.datetime) -> float:
        if self.session_is_open(now_ist):
            return 0.0

        target_date = now_ist.date()
        if now_ist.time() > self.session_end:
            target_date += dt.timedelta(days=1)

        while not self.is_trading_day(target_date):
            target_date += dt.timedelta(days=1)

        target_dt = dt.datetime.combine(target_date, self.session_start, tzinfo=IST)
        return max(0.0, (target_dt - now_ist).total_seconds())

    @staticmethod
    def _time_to_minute_int(value: dt.time) -> int:
        return value.hour * 10000 + value.minute * 100