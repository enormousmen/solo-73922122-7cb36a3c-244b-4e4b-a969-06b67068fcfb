"""会话读写服务。

读路径（已登录用户沿用会话的关键）：
  1. 先查新库 —— 命中即为准（包括“已注销”墓碑），绝不再回旧库；
  2. 新库没有且旧库回读开启时，才回旧库读（尚未搬入、未在新库改动的会话）。

写路径（续期/注销）：只写新库，旧库永远不被修改。
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Callable

import redis

from . import config
from .db import new_client, old_client


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _decode(raw):
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return {"_raw": raw}


class SessionInvalid(Exception):
    """会话无效（不存在/已过期/已注销），不能续期或注销。"""

    def __init__(self, view: dict):
        super().__init__(view.get("reason") or "invalid")
        self.view = view


class SessionService:
    def __init__(self, fallback_enabled: Callable[[], bool]):
        self._fallback_enabled = fallback_enabled

    # ---------- 查询 ----------
    def lookup(self, sid: str) -> dict:
        key = config.SESSION_PREFIX + sid
        now = time.time()
        new = new_client()
        raw = new.get(key)
        if raw is not None:
            data = _decode(raw)
            if isinstance(data, dict) and data.get("revoked"):
                # 墓碑：已注销。以新库为准，不回旧库。
                return self._view(sid, valid=False, reason="revoked", source="new", data=data)
            return self._live_view(sid, "new", data, new.pttl(key), now)
        if self._fallback_enabled():
            try:
                old = old_client()
                raw = old.get(key)
            except redis.RedisError as exc:
                return self._view(sid, valid=False,
                                  reason=f"old-unavailable({exc.__class__.__name__})",
                                  source=None, data=None)
            if raw is not None:
                data = _decode(raw)
                return self._live_view(sid, "old", data, old.pttl(key), now)
        return self._view(sid, valid=False, reason="not-found", source=None, data=None)

    def _live_view(self, sid, source, data, pttl, now):
        if pttl == -2 or pttl == 0:  # 读到后刚好过期
            return self._view(sid, valid=False, reason="expired", source=source, data=data)
        if pttl == -1:  # 无过期时间
            return self._view(sid, valid=True, reason=None, source=source, data=data,
                              expires_at=None, ttl_seconds=None)
        return self._view(sid, valid=True, reason=None, source=source, data=data,
                          expires_at=_iso(now + pttl / 1000), ttl_seconds=pttl // 1000)

    @staticmethod
    def _view(sid, valid, reason, source, data, expires_at=None, ttl_seconds=None):
        return {"sid": sid, "valid": valid, "reason": reason, "source": source,
                "expires_at": expires_at, "ttl_seconds": ttl_seconds, "data": data}

    # ---------- 续期：只写新库，到期时刻 = 原剩余 + 延长 ----------
    def renew(self, sid: str, extend_seconds: int) -> dict:
        cur = self.lookup(sid)
        if not cur["valid"]:
            raise SessionInvalid(cur)
        key = config.SESSION_PREFIX + sid
        src = new_client() if cur["source"] == "new" else old_client()
        pttl = src.pttl(key)
        if pttl == -2 or pttl == 0:  # 查询后刚好过期，不允许“复活”
            raise SessionInvalid(self._view(sid, valid=False, reason="expired",
                                            source=cur["source"], data=cur["data"]))
        data = dict(cur["data"]) if isinstance(cur["data"], dict) else {"_raw": cur["data"]}
        data["renewed_at"] = _iso(time.time())
        data["renew_count"] = int(data.get("renew_count", 0)) + 1
        payload = json.dumps(data, ensure_ascii=False)
        new = new_client()
        if pttl > 0:
            new.set(key, payload, px=pttl + extend_seconds * 1000)
        else:  # 原本不过期的会话保持不过期
            new.set(key, payload)
        return self.lookup(sid)

    # ---------- 注销：在新库写墓碑（保留到原到期时刻） ----------
    def revoke(self, sid: str) -> dict:
        cur = self.lookup(sid)
        if not cur["valid"]:
            raise SessionInvalid(cur)
        key = config.SESSION_PREFIX + sid
        src = new_client() if cur["source"] == "new" else old_client()
        pttl = src.pttl(key)
        if pttl == -2 or pttl == 0:
            raise SessionInvalid(self._view(sid, valid=False, reason="expired",
                                            source=cur["source"], data=cur["data"]))
        tomb = {
            "sid": sid,
            "user": cur["data"].get("user") if isinstance(cur["data"], dict) else None,
            "revoked": True,
            "revoked_at": _iso(time.time()),
        }
        payload = json.dumps(tomb, ensure_ascii=False)
        new = new_client()
        if pttl > 0:
            # 墓碑只需活到原到期时刻，之后旧库里的会话本身也过期了
            new.set(key, payload, px=pttl)
        else:
            new.set(key, payload)
        return self.lookup(sid)

    # ---------- 列表（网页用）：新旧两库合并视图 ----------
    def list_sessions(self, limit: int = 200) -> list[dict]:
        sids: set[str] = set()
        new = new_client()
        for k in new.scan_iter(match=config.SESSION_PREFIX + "*", count=100):
            sids.add(k[len(config.SESSION_PREFIX):])
        if self._fallback_enabled():
            try:
                old = old_client()
                for k in old.scan_iter(match=config.SESSION_PREFIX + "*", count=100):
                    sids.add(k[len(config.SESSION_PREFIX):])
            except redis.RedisError:
                pass  # 旧库断开时只列新库
        out = []
        for sid in sorted(sids)[:limit]:
            v = self.lookup(sid)
            user = v["data"].get("user") if isinstance(v["data"], dict) else None
            out.append({"sid": sid, "user": user, "valid": v["valid"],
                        "reason": v["reason"], "source": v["source"],
                        "expires_at": v["expires_at"]})
        return out
