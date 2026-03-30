from __future__ import annotations

import argparse
import csv
import json
import sys
import threading
from collections import defaultdict
from pathlib import Path
from queue import Empty, Queue
from urllib.parse import urlparse

import requests


DEFAULT_SYMBOLS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"]
DEFAULT_URL = "http://172.105.40.96:8082/docs"
RAW_INDEX_SYMBOLS = {"INDIA_VIX"}
PREFERRED_COLUMNS = [
    "symbol",
    "minute_str",
    "minute_int",
    "updated_at_ms",
    "spot_price",
    "strike",
    "straddle_price",
    "open",
    "high",
    "low",
    "close",
    "ce_close",
    "pe_close",
    "ce_volume",
    "pe_volume",
    "volume",
    "source",
    "version",
]


def normalize_base_url(raw_url: str) -> str:
    url = raw_url.strip().rstrip("/")
    if url.endswith("/docs"):
        url = url[: -len("/docs")]
    elif url.endswith("/openapi.json"):
        url = url[: -len("/openapi.json")]

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"Invalid URL: {raw_url}")
    return url


def build_fieldnames(rows: list[dict]) -> list[str]:
    extras: list[str] = []
    seen = set(PREFERRED_COLUMNS)
    for row in rows:
        for key in row.keys():
            if key not in seen:
                extras.append(key)
                seen.add(key)
    return PREFERRED_COLUMNS + extras


def parse_symbols(raw_symbols: str) -> list[str]:
    symbols = [item.strip().upper() for item in raw_symbols.split(",") if item.strip()]
    if not symbols:
        raise ValueError("At least one symbol is required")
    return symbols


def make_row_key(row: dict) -> tuple:
    return (
        row.get("symbol"),
        row.get("minute_int"),
        row.get("updated_at_ms"),
        row.get("version"),
    )


def endpoint_prefix(symbol: str) -> str:
    return "index" if symbol.upper() in RAW_INDEX_SYMBOLS else "straddle"


def fetch_history(base_url: str, symbol: str, limit: int, timeout: int) -> list[dict]:
    url = f"{base_url}/{endpoint_prefix(symbol)}/history/{symbol}?limit={limit}"
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        print(f"Skipping history fetch for {symbol}: HTTP {status}", file=sys.stderr)
        return []
    payload = response.json()
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def write_rows(output_file: Path, rows: list[dict], append: bool, dedupe: set[tuple]) -> None:
    if not rows:
        return

    output_file.parent.mkdir(parents=True, exist_ok=True)
    file_exists = output_file.exists()
    mode = "a" if append else "w"
    fieldnames = build_fieldnames(rows)

    with output_file.open(mode, newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        if not append or not file_exists:
            writer.writeheader()
        for row in rows:
            key = make_row_key(row)
            if key in dedupe:
                continue
            dedupe.add(key)
            writer.writerow(row)


def _stream_worker(base_url: str, symbol: str, timeout: int, queue: Queue, stop_event: threading.Event) -> None:
    url = f"{base_url}/{endpoint_prefix(symbol)}/stream/{symbol}"
    try:
        with requests.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()
            current_event: dict[str, str] = {}
            for raw_line in response.iter_lines(decode_unicode=True):
                if stop_event.is_set():
                    break
                if raw_line is None:
                    continue
                line = raw_line.strip()

                if not line:
                    if current_event.get("event") == "update" and "data" in current_event:
                        try:
                            row = json.loads(current_event["data"])
                        except json.JSONDecodeError:
                            current_event = {}
                            continue
                        queue.put((symbol, row))
                    current_event = {}
                    continue

                if line.startswith(":"):
                    continue
                if line.startswith("event:"):
                    current_event["event"] = line.split(":", 1)[1].strip()
                elif line.startswith("data:"):
                    current_event["data"] = line.split(":", 1)[1].strip()
    except Exception as exc:  # noqa: BLE001
        queue.put((symbol, {"__error__": str(exc)}))


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream straddle SSE data into a CSV in real time")
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help="Base API URL or docs URL (e.g., http://172.105.40.96:8082/docs)",
    )
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma-separated symbols to stream (e.g., NIFTY,BANKNIFTY)",
    )
    parser.add_argument(
        "--output",
        default="straddle_live.csv",
        help="Output CSV file path",
    )
    parser.add_argument(
        "--history-limit",
        type=int,
        default=100,
        help="Fetch this many recent rows before streaming (set 0 to skip)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds",
    )
    args = parser.parse_args()

    base_url = normalize_base_url(args.url)
    symbols = parse_symbols(args.symbols)
    output_file = Path(args.output).expanduser()

    seen_rows: dict[str, set[tuple]] = defaultdict(set)
    if args.history_limit > 0:
        rows: list[dict] = []
        for symbol in symbols:
            history_rows = fetch_history(base_url, symbol, args.history_limit, args.timeout)
            for row in history_rows:
                key = make_row_key(row)
                if key in seen_rows[symbol]:
                    continue
                seen_rows[symbol].add(key)
                rows.append(row)
        write_rows(output_file, rows, append=False, dedupe=set())
        print(f"Seeded {len(rows)} history rows for {','.join(symbols)} into {output_file}")
    else:
        if output_file.exists() and output_file.stat().st_size > 0:
            print(f"Appending to existing file {output_file}")

    stop_event = threading.Event()
    work_queue: Queue[tuple[str, dict]] = Queue()
    threads: list[threading.Thread] = []
    for symbol in symbols:
        t = threading.Thread(
            target=_stream_worker,
            args=(base_url, symbol, args.timeout, work_queue, stop_event),
            daemon=True,
            name=f"stream-{symbol}",
        )
        t.start()
        threads.append(t)

    print(
        f"Streaming live updates for {','.join(symbols)} from {base_url} into {output_file}. Ctrl+C to stop."
    )

    fieldnames: list[str] | None = None
    wrote_header = output_file.exists() and output_file.stat().st_size > 0

    try:
        with output_file.open("a", newline="", encoding="utf-8") as fh:
            writer: csv.DictWriter | None = None
            while True:
                try:
                    symbol, row = work_queue.get(timeout=1)
                except Empty:
                    if all(not t.is_alive() for t in threads):
                        break
                    continue

                if isinstance(row, dict) and "__error__" in row:
                    print(f"[{symbol}] stream error: {row['__error__']}", file=sys.stderr)
                    continue

                key = make_row_key(row)
                bucket = seen_rows.setdefault(symbol, set())
                if key in bucket:
                    continue
                bucket.add(key)

                if fieldnames is None:
                    fieldnames = build_fieldnames([row])
                    writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
                    if not wrote_header:
                        writer.writeheader()
                        wrote_header = True
                if writer is None:
                    raise RuntimeError("CSV writer was not initialized")

                writer.writerow(row)
                fh.flush()
                value = row.get("straddle_price", row.get("close"))
                print(f"[{symbol}] {row.get('minute_str')} {value}")
    except KeyboardInterrupt:
        print("Stopping streams...")
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=2)


if __name__ == "__main__":
    main()
