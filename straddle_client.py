from __future__ import annotations

import argparse
import json

import redis

try:
    from .redis_keys import current_key, history_key, update_channel
    from .settings import REDIS_URL
except ImportError:  # Support direct script execution
    from redis_keys import current_key, history_key, update_channel
    from settings import REDIS_URL

DEFAULT_REDIS_URL = REDIS_URL


def get_current(redis_url: str, symbol: str) -> dict | None:
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    raw = client.get(current_key(symbol))
    return json.loads(raw) if raw else None


def watch_updates(redis_url: str, symbol: str) -> None:
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    pubsub = client.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(update_channel(symbol))
    try:
        for message in pubsub.listen():
            if message["type"] == "message":
                print(message["data"])
    finally:
        pubsub.close()


def get_history(redis_url: str, symbol: str, limit: int) -> list[dict]:
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    start = -limit if limit > 0 else 0
    rows = client.lrange(history_key(symbol), start, -1)
    return [json.loads(row) for row in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description="Read shared straddle snapshots/updates")
    parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    subparsers = parser.add_subparsers(dest="command", required=True)

    current = subparsers.add_parser("current")
    current.add_argument("--symbol", required=True)

    watch = subparsers.add_parser("watch")
    watch.add_argument("--symbol", required=True)

    history = subparsers.add_parser("history")
    history.add_argument("--symbol", required=True)
    history.add_argument("--limit", type=int, default=20)

    args = parser.parse_args()

    if args.command == "current":
        print(json.dumps(get_current(args.redis_url, args.symbol), ensure_ascii=True, indent=2))
    elif args.command == "watch":
        watch_updates(args.redis_url, args.symbol)
    elif args.command == "history":
        print(json.dumps(get_history(args.redis_url, args.symbol, args.limit), ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
