"""Redis 连接管理。

旧库连接允许不可用（迁移完成并关闭回读后，旧库会被断开），
因此所有旧库访问都必须捕获 redis.RedisError。
"""
from __future__ import annotations

import redis

from . import config

_old: redis.Redis | None = None
_new: redis.Redis | None = None


def init_clients() -> None:
    global _old, _new
    common = dict(decode_responses=True, socket_timeout=3, socket_connect_timeout=3)
    _new = redis.Redis.from_url(config.REDIS_NEW_URL, **common)
    _old = redis.Redis.from_url(config.REDIS_OLD_URL, **common)


def old_client() -> redis.Redis:
    assert _old is not None, "clients not initialized"
    return _old


def new_client() -> redis.Redis:
    assert _new is not None, "clients not initialized"
    return _new


def ping_safe(client: redis.Redis) -> bool:
    try:
        return bool(client.ping())
    except redis.RedisError:
        return False
