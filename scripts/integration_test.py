#!/usr/bin/env python3
"""端到端关键操作试跑：完全通过网页所用的 HTTP API 驱动。

用法：
  python3 scripts/integration_test.py before-kill   # 旧库在线：搬运全流程
  python3 scripts/integration_test.py after-kill    # 旧库已断开：查询仍可用
"""
import sys
import time

import httpx

BASE = "http://127.0.0.1:8000"
FAILURES = []


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f"  -- {detail}" if detail else ""))
    if not cond:
        FAILURES.append(name)


def iso_to_ts(s):
    from datetime import datetime, timezone
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()


def main():
    phase = sys.argv[1] if len(sys.argv) > 1 else "before-kill"
    c = httpx.Client(base_url=BASE, timeout=10)

    if phase == "before-kill":
        run_before_kill(c)
    else:
        run_after_kill(c)

    print()
    if FAILURES:
        print(f"== 共 {len(FAILURES)} 项失败: {FAILURES}")
        sys.exit(1)
    print("== 全部通过 ==")


def run_before_kill(c: httpx.Client):
    print("── 0. 网页与连通性 ──")
    r = c.get("/")
    check("迁移页可打开", r.status_code == 200 and "迁移台" in r.text)
    h = c.get("/api/health").json()
    check("新库连通", h["new_reachable"])
    check("旧库连通", h["old_reachable"])

    print("── 1. 搬运前：会话从旧库读出 ──")
    s01 = c.get("/api/sessions/s01").json()
    check("s01 有效", s01["valid"])
    check("s01 来源=old", s01["source"] == "old", s01["source"])
    s03_before = c.get("/api/sessions/s03").json()
    s03_exp0 = iso_to_ts(s03_before["expires_at"])

    print("── 2. 前置校验：未完成不能关回读；无效会话不能续期/注销 ──")
    r = c.post("/api/migration/fallback/disable")
    check("搬运未完成时关闭回读被拒(409)", r.status_code == 409)
    r = c.post("/api/sessions/ghost/renew", json={"extend_seconds": 60})
    check("不存在的会话续期被拒(409)", r.status_code == 409)
    r = c.post("/api/sessions/ghost/revoke")
    check("不存在的会话注销被拒(409)", r.status_code == 409)

    print("── 3. 读取一批后暂停写入（持有批次） ──")
    st = c.post("/api/migration/step/read").json()
    check("状态=paused", st["status"] == "paused", st["status"])
    held = st["held"]
    check("有持有批次", held and held["count"] >= 2, str(held))
    held_sids = held["sids"]
    print("     持有批次:", held_sids)

    print("── 4. 暂停期间：查询/续期/注销照常 ──")
    # 4a. 持有批次内：续期一个、注销一个
    cand = [s for s in held_sids if s != "short-lived"]
    renew_a, revoke_a = cand[0], cand[1]
    before = c.get(f"/api/sessions/{renew_a}").json()
    r = c.post(f"/api/sessions/{renew_a}/renew", json={"extend_seconds": 1800})
    after = r.json()
    check("持有批次内会话可续期", r.status_code == 200 and after["valid"])
    check("续期后来源=new", after["source"] == "new")
    dt = iso_to_ts(after["expires_at"]) - iso_to_ts(before["expires_at"])
    check("续期延长约 1800s", 1790 < dt < 1810, f"dt={dt:.0f}s")
    r = c.post(f"/api/sessions/{revoke_a}/revoke")
    check("持有批次内会话可注销", r.status_code == 200)
    v = c.get(f"/api/sessions/{revoke_a}").json()
    check("注销后查询无效(revoked)", not v["valid"] and v["reason"] == "revoked")
    r = c.post(f"/api/sessions/{revoke_a}/revoke")
    check("重复注销被拒(409)", r.status_code == 409)

    # 4b. 尚未被搬动（不在持有批次、仍只在旧库）的会话：续期一个、注销一个
    lst = c.get("/api/sessions").json()["sessions"]
    pool = [s["sid"] for s in lst
            if s["source"] == "old" and s["sid"] not in held_sids
            and s["sid"] != "short-lived" and s["valid"]]
    check("还有仅在旧库的会话可操作", len(pool) >= 2, str(pool))
    renew_b, revoke_b = pool[0], pool[1]
    r = c.post(f"/api/sessions/{renew_b}/renew", json={"extend_seconds": 900})
    check("旧库会话暂停期间可续期(写新库)", r.status_code == 200 and r.json()["source"] == "new")
    r = c.post(f"/api/sessions/{revoke_b}/revoke")
    check("旧库会话暂停期间可注销", r.status_code == 200)
    v = c.get(f"/api/sessions/{revoke_b}").json()
    check("注销后以新库为准", not v["valid"] and v["reason"] == "revoked")

    # 4c. 暂停期间短会话仍可查（此刻应仍有效）
    v = c.get("/api/sessions/short-lived").json()
    check("暂停期间可查短会话有效状态与到期时刻", v["valid"] and v["expires_at"], str(v.get("reason")))

    print("── 5. 写入持有批次：续期/注销的不被覆盖 ──")
    st = c.post("/api/migration/step/write").json()
    check("批次写入后跳过已存在=2", st["skipped_existing"] == 2, str(st["skipped_existing"]))
    check("批次新搬=持有数-2", st["migrated"] == held["count"] - 2, str(st["migrated"]))

    print("── 6. 让短会话在“已读未写”的持有批次里过期 ──")
    # 逐批读取，直到 short-lived 进入持有批次
    tries = 0
    while True:
        st = c.post("/api/migration/step/read").json()
        if st["held"] is None:
            raise SystemExit("short-lived 未被读到就扫描完了？！")
        tries += 1
        if "short-lived" in st["held"]["sids"]:
            print(f"     short-lived 已在持有批次（第 {tries} 次读取）: {st['held']['sids']}")
            break
        c.post("/api/migration/step/write")  # 不含目标，直接写入继续
        if tries > 30:
            raise SystemExit("读取批次次数异常")
    # 等到它过期（暂停期间照常查询其状态变化）
    deadline = time.time() + 120
    expired_seen = False
    while time.time() < deadline:
        v = c.get("/api/sessions/short-lived").json()
        if not v["valid"]:
            expired_seen = True
            break
        time.sleep(2)
    check("暂停期间短会话自然过期（查询可见）", expired_seen)
    st = c.post("/api/migration/step/write").json()
    check("过期会话被丢弃(expired>=1)", st["expired"] >= 1, str(st["expired"]))
    v = c.get("/api/sessions/short-lived").json()
    check("过期会话未被搬入新库(不能恢复)", not v["valid"])

    print("── 7. 继续自动搬运至完成 ──")
    c.post("/api/migration/start")
    deadline = time.time() + 60
    st = {}
    while time.time() < deadline:
        st = c.get("/api/migration/status").json()
        if st["status"] == "done":
            break
        time.sleep(0.5)
    check("搬运完成", st["status"] == "done", st["status"])
    print(f"     计数: migrated={st['migrated']} skipped={st['skipped_existing']} "
          f"expired={st['expired']} batches={st['batches']}")
    check("全程新搬 20 条", st["migrated"] == 20, str(st["migrated"]))
    check("全程跳过 4 条(2续期+2注销)", st["skipped_existing"] == 4, str(st["skipped_existing"]))
    check("全程过期丢弃 1 条", st["expired"] == 1, str(st["expired"]))

    print("── 8. 搬运保留原到期时刻；新库为准 ──")
    s03 = c.get("/api/sessions/s03").json()
    check("s03 来源=new", s03["source"] == "new")
    drift = abs(iso_to_ts(s03["expires_at"]) - s03_exp0)
    check("s03 到期时刻保留(漂移<10s)", drift < 10, f"drift={drift:.1f}s")
    v = c.get(f"/api/sessions/{renew_a}").json()
    check("续期会话仍有效且在新库", v["valid"] and v["source"] == "new")
    v = c.get(f"/api/sessions/{revoke_a}").json()
    check("注销会话仍无效", not v["valid"] and v["reason"] == "revoked")

    print("── 9. 重试搬运：不能把新库改回旧状态 ──")
    c.post("/api/migration/reset")
    c.post("/api/migration/start")
    deadline = time.time() + 60
    while time.time() < deadline:
        st = c.get("/api/migration/status").json()
        if st["status"] == "done":
            break
        time.sleep(0.5)
    check("重跑完成", st["status"] == "done")
    check("重跑新搬 0 条", st["migrated"] == 0, str(st["migrated"]))
    check("重跑全部跳过(24)", st["skipped_existing"] == 24, str(st["skipped_existing"]))
    v = c.get(f"/api/sessions/{renew_a}").json()
    check("重跑后续期状态未被改回", v["valid"] and v["source"] == "new"
          and v["data"].get("renew_count") == 1)
    v = c.get(f"/api/sessions/{revoke_a}").json()
    check("重跑后注销状态未被改回", not v["valid"] and v["reason"] == "revoked")

    print("── 10. 关闭旧库回读 ──")
    r = c.post("/api/migration/fallback/disable")
    check("关闭回读成功", r.status_code == 200)
    st = c.get("/api/migration/status").json()
    check("回读已关闭", not st["fallback_enabled"])
    v = c.get("/api/sessions/s01").json()
    check("关闭回读后查询正常(新库)", v["valid"] and v["source"] == "new")
    print()
    print(">> 现在可以断开旧库（docker stop redis-old），然后运行 after-kill 阶段")


def run_after_kill(c: httpx.Client):
    print("── 旧库已断开：查询/续期/注销仍正常 ──")
    h = c.get("/api/health").json()
    check("旧库已断开", not h["old_reachable"])
    check("新库仍连通", h["new_reachable"])
    st = c.get("/api/migration/status").json()
    check("迁移状态可查(done)", st["status"] == "done")
    check("回读保持关闭", not st["fallback_enabled"])

    v = c.get("/api/sessions/s05").json()
    check("已搬入会话有效(新库)", v["valid"] and v["source"] == "new", str(v.get("reason")))
    exp_before = v["expires_at"]

    r = c.post("/api/sessions/s05/renew", json={"extend_seconds": 60})
    check("旧库断开后续期正常", r.status_code == 200 and r.json()["valid"])
    check("续期后到期时刻更晚", iso_to_ts(r.json()["expires_at"]) > iso_to_ts(exp_before))

    lst = c.get("/api/sessions").json()["sessions"]
    revoked = [s for s in lst if s["reason"] == "revoked"]
    check("注销会话仍无效", len(revoked) >= 2, str(len(revoked)))

    v = c.get("/api/sessions/short-lived").json()
    check("过期会话不会被恢复", not v["valid"])
    v = c.get("/api/sessions/ghost").json()
    check("陌生会话无效", not v["valid"] and v["reason"] == "not-found")
    r = c.get("/")
    check("迁移页仍可打开", r.status_code == 200)


if __name__ == "__main__":
    main()
