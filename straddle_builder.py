from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

try:
    from .contract_data import SYMBOL_CONFIG
except ImportError:  # Support direct script execution
    from contract_data import SYMBOL_CONFIG


@dataclass
class StraddleSelection:
    symbol: str
    fo_segment: str
    expiry_rank: int
    spot_price: float
    strike: float
    ce_instrument_key: str
    pe_instrument_key: str
    ce_exchange_token: str
    pe_exchange_token: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def atm_strike(symbol: str, spot_price: float) -> float:
    step = SYMBOL_CONFIG[symbol]["strike_step"]
    return float(round(spot_price / step) * step)


def _find_strike_row(strike_tree: dict[Any, dict[str, Any]], strike: float) -> dict[str, Any]:
    candidates = (
        strike,
        float(strike),
        int(strike),
        str(int(strike)),
        str(float(strike)),
    )
    for key in candidates:
        if key in strike_tree:
            return strike_tree[key]
    raise KeyError(f"strike {strike} not found")


def build_straddle_for_strike(
    segment_wise_contract_dict: dict[str, Any],
    symbol: str,
    spot_price: float,
    strike: float,
) -> StraddleSelection:
    config = SYMBOL_CONFIG[symbol]
    fo_tree = segment_wise_contract_dict[config["fo_segment"]][symbol]
    ce_row = _find_strike_row(fo_tree["CE"][1], strike)
    pe_row = _find_strike_row(fo_tree["PE"][1], strike)

    return StraddleSelection(
        symbol=symbol,
        fo_segment=config["fo_segment"],
        expiry_rank=1,
        spot_price=float(spot_price),
        strike=float(strike),
        ce_instrument_key=str(ce_row["instrument_key"]),
        pe_instrument_key=str(pe_row["instrument_key"]),
        ce_exchange_token=str(ce_row["exchange_token"]),
        pe_exchange_token=str(pe_row["exchange_token"]),
    )


def build_atm_straddle(segment_wise_contract_dict: dict[str, Any], symbol: str, spot_price: float) -> StraddleSelection:
    return build_straddle_for_strike(
        segment_wise_contract_dict=segment_wise_contract_dict,
        symbol=symbol,
        spot_price=spot_price,
        strike=atm_strike(symbol, spot_price),
    )


def build_from_pubsub_event(segment_wise_contract_dict: dict[str, Any], event: dict[str, Any]) -> StraddleSelection:
    token = str(event.get("token") or "")
    for symbol, config in SYMBOL_CONFIG.items():
        if token == config["pubsub_spot_token"]:
            return build_atm_straddle(segment_wise_contract_dict, symbol, float(event["close"]))
    raise KeyError(f"unsupported pub/sub spot token: {token}")
