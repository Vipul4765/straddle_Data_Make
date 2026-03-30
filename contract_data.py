from __future__ import annotations

import argparse
import datetime as dt
import gzip
import io
import json
from pathlib import Path
from typing import Any

import requests

try:
    from .settings import (
        STRADDLE_CONTRACT_OUTPUT_FILE,
        STRADDLE_CONTRACT_TIMEOUT,
        STRADDLE_CONTRACT_URL,
        SYMBOL_CONFIG,
    )
except ImportError:  # Support direct script execution
    from settings import (
        STRADDLE_CONTRACT_OUTPUT_FILE,
        STRADDLE_CONTRACT_TIMEOUT,
        STRADDLE_CONTRACT_URL,
        SYMBOL_CONFIG,
    )

CONTRACT_URL = STRADDLE_CONTRACT_URL
OUTPUT_FILE = Path(STRADDLE_CONTRACT_OUTPUT_FILE)
EXTRA_INDEX_ROWS = (
    {
        "segment": "NSE_INDEX",
        "storage_name": "INDIA VIX",
        "display_names": {"INDIA VIX", "India VIX"},
        "trading_symbols": {"INDIA_VIX", "India_VIX", "INDIA VIX"},
    },
)


def _expiry_datetime(row: dict[str, Any]) -> dt.datetime:
    return dt.datetime.fromtimestamp(int(row["expiry"]) / 1000)


def load_contracts(data: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """
    Keep only nearest expiry (rank 1) for FUT/CE/PE chains.

    Result shape:
        segment_wise_contract_dict["NSE_FO"]["NIFTY"]["CE"][1][22750.0]
    """
    segment_wise_contract_dict: dict[str, Any] = {
        "NSE_FO": {},
        "BSE_FO": {},
        "NSE_INDEX": {},
        "BSE_INDEX": {},
    }
    token_contract_dict: dict[str, dict[str, Any]] = {}
    nearest_expiry_by_chain: dict[tuple[str, str, str], dt.datetime] = {}

    for row in data:
        seg = row.get("segment")
        symbol = row.get("asset_symbol")
        inst_type = row.get("instrument_type")
        if seg not in {"NSE_FO", "BSE_FO"} or symbol not in SYMBOL_CONFIG or inst_type not in {"FUT", "CE", "PE"}:
            continue
        expiry_dt = _expiry_datetime(row)
        chain_key = (seg, symbol, inst_type)
        current = nearest_expiry_by_chain.get(chain_key)
        if current is None or expiry_dt < current:
            nearest_expiry_by_chain[chain_key] = expiry_dt

    for row in data:
        seg = row.get("segment")
        exchange_token = row.get("exchange_token")

        if seg in {"NSE_FO", "BSE_FO"}:
            symbol = row.get("asset_symbol")
            inst_type = row.get("instrument_type")
            if symbol not in SYMBOL_CONFIG or inst_type not in {"FUT", "CE", "PE"}:
                continue

            expiry_dt = _expiry_datetime(row)
            if expiry_dt != nearest_expiry_by_chain.get((seg, symbol, inst_type)):
                continue

            inst_tree = segment_wise_contract_dict[seg].setdefault(symbol, {}).setdefault(inst_type, {})
            if inst_type == "FUT":
                inst_tree[1] = row
            else:
                strike = float(row["strike_price"])
                inst_tree.setdefault(1, {})[strike] = row

            if exchange_token:
                token_contract_dict[str(exchange_token)] = row

        elif seg in {"NSE_INDEX", "BSE_INDEX"}:
            trading_symbol = row.get("trading_symbol")
            display_name = row.get("name")
            matched = False
            for config in SYMBOL_CONFIG.values():
                if seg != config["spot_segment"]:
                    continue
                if display_name != config["spot_name"] and trading_symbol != config["spot_trading_symbol"]:
                    continue

                segment_wise_contract_dict[seg][config["spot_name"]] = row
                if exchange_token:
                    token_contract_dict[str(exchange_token)] = row
                matched = True
                break

            if matched:
                continue

            for extra in EXTRA_INDEX_ROWS:
                if seg != extra["segment"]:
                    continue
                if display_name not in extra["display_names"] and trading_symbol not in extra["trading_symbols"]:
                    continue

                segment_wise_contract_dict[seg][extra["storage_name"]] = row
                if exchange_token:
                    token_contract_dict[str(exchange_token)] = row
                break

    return segment_wise_contract_dict, token_contract_dict


def contract_file(timeout: int = STRADDLE_CONTRACT_TIMEOUT) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    response = requests.get(CONTRACT_URL, timeout=timeout)
    response.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
        data = json.loads(gz.read().decode("utf-8"))
    return load_contracts(data)


def build_pub_sub_tokens(segment_wise_contract_dict: dict[str, Any]) -> list[str]:
    seen: set[str] = set()
    tokens: list[str] = []

    def add_token(instrument_key: str | None) -> None:
        if not instrument_key or instrument_key in seen:
            return
        seen.add(instrument_key)
        tokens.append(instrument_key)

    for symbol, config in SYMBOL_CONFIG.items():
        spot_row = segment_wise_contract_dict[config["spot_segment"]].get(config["spot_name"])
        if isinstance(spot_row, dict):
            add_token(spot_row.get("instrument_key"))

        inst_tree = segment_wise_contract_dict[config["fo_segment"]].get(symbol, {})
        fut_row = inst_tree.get("FUT", {}).get(1)
        if isinstance(fut_row, dict):
            add_token(fut_row.get("instrument_key"))

        for option_type in ("CE", "PE"):
            strike_tree = inst_tree.get(option_type, {}).get(1, {})
            for strike in sorted(strike_tree):
                add_token(strike_tree[strike].get("instrument_key"))

    india_vix_row = segment_wise_contract_dict["NSE_INDEX"].get("INDIA VIX")
    if isinstance(india_vix_row, dict):
        add_token(india_vix_row.get("instrument_key"))

    return tokens


def write_pub_sub_tokens_file(output_file: Path = OUTPUT_FILE, timeout: int = STRADDLE_CONTRACT_TIMEOUT) -> Path:
    segment_wise_contract_dict, _ = contract_file(timeout=timeout)
    output_file.write_text("\n".join(build_pub_sub_tokens(segment_wise_contract_dict)) + "\n", encoding="utf-8")
    return output_file


def main() -> None:
    parser = argparse.ArgumentParser(description="Write nearest-expiry pub/sub token file")
    parser.add_argument("--output", default=str(OUTPUT_FILE))
    parser.add_argument("--timeout", type=int, default=STRADDLE_CONTRACT_TIMEOUT)
    args = parser.parse_args()

    output_path = write_pub_sub_tokens_file(Path(args.output), timeout=args.timeout)
    print(output_path)


if __name__ == "__main__":
    main()
