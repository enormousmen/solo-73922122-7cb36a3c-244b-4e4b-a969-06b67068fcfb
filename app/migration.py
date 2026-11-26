"""分批搬运引擎：旧库 -> 新库。

不变式：
  * 旧库永远只读（应用层不写；compose 中再用 ACL 强制）；
  * 新库写入一律 SET ... NX —— 已存在的键（含续期后的新值、注销墓碑）绝不覆盖，
    因此继续/重试搬运都不可能把新库状态改回旧状态；
  * 搬运保留原到期时刻：写入时 PX = 读出时的 PTTL - 已流逝时间，
    暂停期间过期的会话在写入时被丢弃，不会复活；
  * SCAN 游标在对应批次写入成功后才提交到新库，暂停/重启不丢会话。
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

import redis

from . import config
from .db import new_client, old_client


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


class MigrationConflict(Exception):
    """操作与当前迁移状态冲突（如搬运中要求暂停之外的步骤）。"""


class MigrationEngine:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pause_req = threading.Event()
        self._worker: threading.Thread | None = None
        # 已读出、尚未写入的一批（暂停写入期间的“持有批次”）。
        # 游标尚未提交，进程重启也不会丢：重新读取会再次扫到。
        self._held: dict | None = None

    # ---------- 状态（存新库，重启不丢） ----------
    def _state(self) -> dict:
        raw = new_client().hgetall(config.STATE_KEY)
        return {
            "status": raw.get("status", "idle"),
            "cursor": raw.get("cursor", "0"),
            "migrated": int(raw.get("migrated", 0)),
            "skipped_existing": int(raw.get("skipped_existing", 0)),
            "expired": int(raw.get("expired", 0)),
            "batches": int(raw.get("batches", 0)),
            "fallback_enabled": raw.get("fallback_enabled", "1") == "1",
            "message": raw.get("message", ""),
            "updated_at": raw.get("updated_at", ""),
        }

    def _save(self, **fields) -> None:
        mapping = {k: (v if isinstance(v, str) else str(v)) for k, v in fields.items()}
        mapping["updated_at"] = _iso(time.time())
        new_client().hset(config.STATE_KEY, mapping=mapping)

    def _log(self, msg: str) -> None:
        new = new_client()
        new.rpush(config.LOG_KEY, f"{_iso(time.time())} {msg}")
        new.ltrim(config.LOG_KEY, -200, -1)

    def fallback_enabled(self) -> bool:
        return self._state()["fallback_enabled"]

    def status(self) -> dict:
        st = self._state()
        held = None
        if self._held is not None:
            held = {
                "count": len(self._held["items"]),
                "sids": [it["key"][len(config.SESSION_PREFIX):] for it in self._held["items"]],
                "read_at": self._held["read_at_wall"],
            }
        try:
            logs = new_client().lrange(config.LOG_KEY, -30, -1)
        except redis.RedisError:
            logs = []
        st["held"] = held
        st["log"] = list(reversed(logs))
        return st

    # ---------- 网页操作 ----------
    def start(self) -> dict:
        """开始/继续自动搬运（后台线程逐批读+写，直到完成或被暂停）。"""
        with self._lock:
            st = self._state()
            if st["status"] == "running":
                raise MigrationConflict("搬运正在进行中")
            if st["status"] == "done":
                raise MigrationConflict("搬运已完成；如需重跑请先“重置搬运”")
            self._pause_req.clear()
            self._save(status="running", message="自动搬运中")
            self._log("开始/继续自动搬运")
            self._worker = threading.Thread(target=self._run_auto, daemon=True,
                                            name="migration-worker")
            self._worker.start()
        return self.status()

    def pause(self) -> dict:
        """请求暂停：当前批次读出后、写入前停下（批次保持“持有”）。"""
        with self._lock:
            if self._state()["status"] != "running":
                raise MigrationConflict("当前不在自动搬运中")
            self._pause_req.set()
            self._log("请求暂停：当前批次读完后暂停写入")
        return self.status()

    def step_read(self) -> dict:
        """手动单步：从旧库读出一批并保持（不写入）。"""
        with self._lock:
            st = self._state()
            if st["status"] == "running":
                raise MigrationConflict("自动搬运进行中，请先暂停")
            if st["status"] == "done":
                raise MigrationConflict("搬运已完成；如需重跑请先“重置搬运”")
            if self._held is not None:
                raise MigrationConflict("已有一批读入待写入，请先写入或丢弃")
            self._read_batch_locked()
            if self._held is None:
                self._save(status="done", cursor="0", message="旧库已无可搬会话")
                self._log("旧库扫描完毕，无更多会话，搬运完成 ✅")
            else:
                n = len(self._held["items"])
                self._save(status="paused", message=f"已读入 {n} 条，写入暂停中")
        return self.status()

    def step_write(self) -> dict:
        """手动单步：把持有的一批写入新库。"""
        with self._lock:
            if self._held is None:
                raise MigrationConflict("没有待写入的批次")
            self._write_held_locked()
            if self._state()["cursor"] == "0":
                self._save(status="done", message="全部批次写入完成")
                self._log("搬运完成 ✅")
            else:
                self._save(status="paused", message="批次已写入，处于暂停")
        return self.status()

    def step_discard(self) -> dict:
        """丢弃持有批次（游标未提交，重读时会再次扫到，不丢会话）。"""
        with self._lock:
            if self._held is None:
                raise MigrationConflict("没有持有中的批次")
            n = len(self._held["items"])
            self._held = None
            self._save(status="paused", message="已丢弃持有批次（游标未前进）")
            self._log(f"丢弃持有批次（{n} 条），游标未提交，重新读取时会再次扫到")
        return self.status()

    def reset(self) -> dict:
        """重置计数与游标，用于搬运完成后验证“重试不改回旧状态”。"""
        with self._lock:
            if self._state()["status"] == "running":
                raise MigrationConflict("自动搬运进行中，请先暂停")
            self._held = None
            self._save(status="idle", cursor="0", migrated=0, skipped_existing=0,
                       expired=0, batches=0,
                       message="已重置，可重新搬运（已存在/墓碑键仍不会被覆盖）")
            self._log("重置搬运状态（计数与游标清零）")
        return self.status()

    def disable_fallback(self) -> dict:
        """搬运完成后关闭旧库回读；此后旧库可安全断开。"""
        with self._lock:
            st = self._state()
            if st["status"] != "done":
                raise MigrationConflict("搬运未完成，不能关闭旧库回读")
            if not st["fallback_enabled"]:
                raise MigrationConflict("旧库回读已关闭")
            self._save(fallback_enabled="0", message="旧库回读已关闭，查询只走新库")
            self._log("关闭旧库回读；此后旧库可安全断开")
        return self.status()

    # ---------- 内部 ----------
    def _run_auto(self) -> None:
        try:
            while True:
                with self._lock:
                    if self._pause_req.is_set():
                        self._save(status="paused", message="已暂停")
                        self._log("自动搬运已暂停")
                        return
                    if self._held is None:
                        self._read_batch_locked()
                        if self._held is None:
                            self._save(status="done", cursor="0",
                                       message="全部批次写入完成")
                            self._log("搬运完成 ✅")
                            return
                    # 读到一批后、写入前再检查一次暂停 —— “读到一批旧会话后可暂停写入”
                    if self._pause_req.is_set():
                        n = len(self._held["items"])
                        self._save(status="paused",
                                   message=f"已读入 {n} 条，写入暂停中")
                        self._log("读到一批后暂停：批次已读入、尚未写入")
                        return
                    self._write_held_locked()
                    if self._state()["cursor"] == "0":
                        self._save(status="done", message="全部批次写入完成")
                        self._log("搬运完成 ✅")
                        return
                time.sleep(config.MIGRATION_BATCH_DELAY_MS / 1000)
        except redis.RedisError as exc:
            with self._lock:
                self._save(status="paused", message=f"出错暂停：{exc!r}")
                self._log(f"搬运出错：{exc!r}")

    def _read_batch_locked(self) -> None:
        """从旧库 SCAN 一批并读出值与 PTTL；结果放入 self._held。"""
        old = old_client()
        cursor = int(self._state()["cursor"])
        items: list[dict] = []
        next_cursor = cursor
        # SCAN + MATCH 可能返回空页，继续扫直到拿到数据或整轮迭代结束
        while True:
            next_cursor, keys = old.scan(cursor=next_cursor,
                                         match=config.SESSION_PREFIX + "*",
                                         count=config.MIGRATION_BATCH_COUNT)
            if keys:
                pipe = old.pipeline(transaction=False)
                for k in keys:
                    pipe.get(k)
                    pipe.pttl(k)
                res = pipe.execute()
                for i, k in enumerate(keys):
                    val, pttl = res[2 * i], res[2 * i + 1]
                    if val is None:
                        continue  # 扫描到读取之间刚好过期
                    items.append({"key": k, "value": val, "pttl_ms": pttl})
            if items or next_cursor == 0:
                break
        if not items:
            self._held = None
            return
        self._held = {
            "items": items,
            "next_cursor": str(next_cursor),
            "read_at": time.monotonic(),
            "read_at_wall": _iso(time.time()),
        }
        self._log(f"读出 {len(items)} 条（游标 {cursor} → {next_cursor}），等待写入")

    def _write_held_locked(self) -> None:
        """把持有批次写入新库：保留原到期时刻；NX 不覆盖；过期丢弃。"""
        new = new_client()
        held = self._held
        assert held is not None
        self._held = None
        elapsed_ms = (time.monotonic() - held["read_at"]) * 1000.0
        migrated = skipped = expired = 0
        skipped_sids: list[str] = []
        expired_sids: list[str] = []
        plen = len(config.SESSION_PREFIX)
        for it in held["items"]:
            key, val, pttl = it["key"], it["value"], it["pttl_ms"]
            if pttl == -1:  # 旧库中无过期时间：原样保留
                if new.set(key, val, nx=True):
                    migrated += 1
                else:
                    skipped += 1
                    skipped_sids.append(key[plen:])
                continue
            remaining = int(pttl - elapsed_ms)
            if remaining <= 0:
                # 暂停期间已过期：丢弃，绝不复活
                expired += 1
                expired_sids.append(key[plen:])
                continue
            if new.set(key, val, px=remaining, nx=True):
                migrated += 1
            else:
                # 新库已有（续期过/已注销墓碑/此前搬过）：以新库为准，跳过
                skipped += 1
                skipped_sids.append(key[plen:])
        st = self._state()
        self._save(cursor=held["next_cursor"],
                   migrated=st["migrated"] + migrated,
                   skipped_existing=st["skipped_existing"] + skipped,
                   expired=st["expired"] + expired,
                   batches=st["batches"] + 1)
        parts = [f"写入批次：新搬 {migrated}"]
        if skipped:
            parts.append(f"跳过已存在 {skipped}（{'、'.join(skipped_sids)}）")
        if expired:
            parts.append(f"过期丢弃 {expired}（{'、'.join(expired_sids)}）")
        self._log("；".join(parts))
