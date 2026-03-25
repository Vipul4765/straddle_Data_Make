from __future__ import annotations

from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # Keep scripts runnable even if python-dotenv is missing
    load_dotenv = None


def load_local_env() -> None:
    """Load .env from this package folder when available."""
    if load_dotenv is None:
        return
    env_path = Path(__file__).resolve().with_name(".env")
    load_dotenv(env_path)

