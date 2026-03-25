"""Utilities for building nearest-expiry ATM straddles from pub/sub data."""

try:
	from .env_loader import load_local_env
except ImportError:  # pragma: no cover
	load_local_env = None

if load_local_env is not None:
	load_local_env()

