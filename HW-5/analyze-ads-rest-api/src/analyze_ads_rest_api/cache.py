"""Helper functions for a simple read‑through Redis cache"""
import json
import os
from typing import Any

import redis

_redis = None


def _get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.Redis(
            host=os.environ.get("REDIS_HOST", "localhost"),
            port=int(os.environ.get("REDIS_PORT", 6379)),
            decode_responses=True,
        )
    return _redis


def cache_get(key: str):
    return json.loads(_get_redis().get(key) or "null")


def cache_set(key: str, value: Any, ttl: int):
    _get_redis().setex(key, ttl, json.dumps(value))
