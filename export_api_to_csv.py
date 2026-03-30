from __future__ import annotations

import argparse
import csv
import threading
import json
import sys
from collections import defaultdict
from queue import Empty, Queue
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import requests


DEFAULT_SYMBOLS = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"]
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


def parse_symbols(raw_symbols: str) -> list[str]:
    symbols = [item.strip().upper() for item in raw_symbols.split(",") if item.strip()]
    if not symbols:
        raise ValueError("At least one symbol is required")
    return symbols


def request_json(url: str, timeout: int) -> dict | list:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()


def build_fieldnames(rows: Iterable[dict]) -> list[str]:
    extras: list[str] = []
    seen = set(PREFERRED_COLUMNS)
    for row in rows:
        for key in row.keys():
            if key not in seen:
                extras.append(key)
                seen.add(key)
    return PREFERRED_COLUMNS + extras


def make_row_key(row: dict) -> tuple:
    return (
        row.get("symbol"),
        row.get("minute_int"),
        row.get("updated_at_ms"),
        row.get("version"),
    )


def write_csv(output_file: Path, rows: list[dict], append: bool = False) -> None:
    if not rows:
        print("No rows to write.")
        return

    output_file.parent.mkdir(parents=True, exist_ok=True)
    file_exists = output_file.exists()
    mode = "a" if append else "w"
    fieldnames = build_fieldnames(rows)

    with output_file.open(mode, newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        if not append or not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def fetch_current(base_url: str, symbols: list[str], timeout: int) -> list[dict]:
    rows: list[dict] = []
    for symbol in symbols:
        url = f"{base_url}/straddle/current/{symbol}"
        try:
            payload = request_json(url, timeout)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            print(f"Skipping {symbol}: HTTP {status}", file=sys.stderr)
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def fetch_history(base_url: str, symbols: list[str], limit: int, timeout: int) -> list[dict]:
    rows: list[dict] = []
    for symbol in symbols:
        url = f"{base_url}/straddle/history/{symbol}?limit={limit}"
        try:
            payload = request_json(url, timeout)
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            print(f"Skipping {symbol}: HTTP {status}", file=sys.stderr)
            continue
        if isinstance(payload, list):
            rows.extend(row for row in payload if isinstance(row, dict))
    return rows


def stream_to_csv(
    base_url: str,
    symbol: str,
    output_file: Path,
    timeout: int,
    seen_keys: set[tuple] | None = None,
) -> None:
    url = f"{base_url}/straddle/stream/{symbol}"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    wrote_header = output_file.exists() and output_file.stat().st_size > 0
    seen = seen_keys if seen_keys is not None else set()

    with requests.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        fieldnames: list[str] | None = None
        current_event: dict[str, str] = {}

        with output_file.open("a", newline="", encoding="utf-8") as fh:
            writer: csv.DictWriter | None = None
            for raw_line in response.iter_lines(decode_unicode=True):
                if raw_line is None:
                    continue
                line = raw_line.strip()

                if not line:
                    if current_event.get("event") == "update" and "data" in current_event:
                        row = json.loads(current_event["data"])
                        row_key = make_row_key(row)
                        if row_key in seen:
                            current_event = {}
                            continue
                        seen.add(row_key)
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
                        print(
                            f"Appended {row.get('symbol')} {row.get('minute_str')} {row.get('straddle_price')}"
                        )
                    current_event = {}
                    continue

                if line.startswith(":"):
                    continue
                if line.startswith("event:"):
                    current_event["event"] = line.split(":", 1)[1].strip()
                elif line.startswith("data:"):
                    current_event["data"] = line.split(":", 1)[1].strip()


def _stream_worker(
    base_url: str,
    symbol: str,
    timeout: int,
    queue: Queue,
    stop_event: threading.Event,
) -> None:
    url = f"{base_url}/straddle/stream/{symbol}"
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


def stream_multi_to_csv(
    base_url: str,
    symbols: list[str],
    output_file: Path,
    timeout: int,
    history_limit: int,
) -> None:
    stop_event = threading.Event()
    work_queue: Queue[tuple[str, dict]] = Queue()
    seen_by_symbol: dict[str, set[tuple]] = defaultdict(set)

    if history_limit > 0:
        history_rows = fetch_history(base_url, symbols, history_limit, timeout)
        filtered_rows: list[dict] = []
        for row in history_rows:
            symbol = str(row.get("symbol") or "").upper()
            key = make_row_key(row)
            if not symbol or key in seen_by_symbol[symbol]:
                continue
            seen_by_symbol[symbol].add(key)
            filtered_rows.append(row)
        write_csv(output_file, filtered_rows, append=False)
        print(f"Seeded {len(filtered_rows)} history rows into {output_file}")

    threads: list[threading.Thread] = []
    for symbol in symbols:
        t = threading.Thread(
            target=_stream_worker,
            args=(base_url, symbol, timeout, work_queue, stop_event),
            daemon=True,
            name=f"stream-{symbol}",
        )
        t.start()
        threads.append(t)

    print(f"Streaming live rows for {','.join(symbols)} from {base_url} into {output_file}. Ctrl+C to stop.")

    fieldnames: list[str] | None = None
    file_exists = output_file.exists() and output_file.stat().st_size > 0
    wrote_header = file_exists

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
                bucket = seen_by_symbol.setdefault(symbol, set())
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
                print(f"[{symbol}] {row.get('minute_str')} {row.get('straddle_price')}")
    except KeyboardInterrupt:
        print("Stopping streams...")
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export straddle API data to CSV")
    parser.add_argument(
        "--url",
        default="http://172.105.40.96:8082/docs",
        help="Base API URL or docs URL, for example http://172.105.40.96:8082/docs",
    )
    parser.add_argument(
        "--mode",
        choices=["current", "history", "stream", "history-stream", "stream-multi"],
        default="history",
        help=(
            "current = one row per symbol, history = many rows per symbol, stream = append live SSE rows, "
            "history-stream = write history first then keep appending live rows, stream-multi = append live SSE rows for all symbols"
        ),
    )
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma-separated symbol list",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="History row limit per symbol for history/history-stream/stream-multi modes",
    )
    parser.add_argument(
        "--output",
        default="straddle_api_export.csv",
        help="Output CSV file path",
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

    if args.mode == "current":
        rows = fetch_current(base_url, symbols, args.timeout)
        write_csv(output_file, rows, append=False)
        print(f"Wrote {len(rows)} current rows to {output_file}")
        return

    if args.mode == "history":
        rows = fetch_history(base_url, symbols, args.limit, args.timeout)
        write_csv(output_file, rows, append=False)
        print(f"Wrote {len(rows)} history rows to {output_file}")
        return

    if args.mode == "history-stream":
        if len(symbols) != 1:
            raise SystemExit("history-stream mode requires exactly one symbol in --symbols")
        rows = fetch_history(base_url, symbols, args.limit, args.timeout)
        write_csv(output_file, rows, append=False)
        seen_keys = {make_row_key(row) for row in rows}
        print(
            f"Wrote {len(rows)} history rows to {output_file}. Now streaming live rows for {symbols[0]}."
        )
        stream_to_csv(base_url, symbols[0], output_file, args.timeout, seen_keys=seen_keys)
        return

    if args.mode == "stream-multi":
        if not symbols:
            raise SystemExit("stream-multi mode requires at least one symbol in --symbols")
        stream_multi_to_csv(
            base_url=base_url,
            symbols=symbols,
            output_file=output_file,
            timeout=args.timeout,
            history_limit=args.limit,
        )
        return

    if len(symbols) != 1:
        raise SystemExit("Stream mode requires exactly one symbol in --symbols")
    print(f"Streaming live rows for {symbols[0]} from {base_url} into {output_file}")
    stream_to_csv(base_url, symbols[0], output_file, args.timeout)


if __name__ == "__main__":
    main()
