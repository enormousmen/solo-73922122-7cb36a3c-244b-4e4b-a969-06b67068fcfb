"""FastAPI 入口：迁移页 + API。"""
from __future__ import annotations

from pathlib import Path

import redis
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from .db import init_clients, new_client, old_client, ping_safe
from .migration import MigrationConflict, MigrationEngine
from .sessions import SessionInvalid, SessionService

STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="Redis 会话迁移台", docs_url=None, redoc_url=None, openapi_url=None)

engine: MigrationEngine
sessions: SessionService


@app.on_event("startup")
def _startup() -> None:
    init_clients()
    global engine, sessions
    engine = MigrationEngine()
    sessions = SessionService(fallback_enabled=engine.fallback_enabled)


# ---------------- 页面 ----------------
@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC_DIR / "index.html")


# ---------------- 健康/状态 ----------------
@app.get("/api/health")
def health():
    return {
        "new_reachable": ping_safe(new_client()),
        "old_reachable": ping_safe(old_client()),
    }


# ---------------- 会话 ----------------
@app.get("/api/sessions")
def list_sessions():
    return {"sessions": sessions.list_sessions()}


@app.get("/api/sessions/{sid}")
def get_session(sid: str):
    view = sessions.lookup(sid)
    view["fallback_enabled"] = engine.fallback_enabled()
    return view


class RenewBody(BaseModel):
    extend_seconds: int = Field(default=1800, ge=1, le=30 * 24 * 3600)


@app.post("/api/sessions/{sid}/renew")
def renew_session(sid: str, body: RenewBody):
    try:
        return sessions.renew(sid, body.extend_seconds)
    except SessionInvalid as exc:
        raise HTTPException(status_code=409,
                            detail={"msg": "会话无效，不能续期", "view": exc.view})
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=f"redis: {exc!r}")


@app.post("/api/sessions/{sid}/revoke")
def revoke_session(sid: str):
    try:
        return sessions.revoke(sid)
    except SessionInvalid as exc:
        raise HTTPException(status_code=409,
                            detail={"msg": "会话无效，不能注销", "view": exc.view})
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=f"redis: {exc!r}")


# ---------------- 迁移 ----------------
@app.get("/api/migration/status")
def migration_status():
    st = engine.status()
    st["new_reachable"] = ping_safe(new_client())
    st["old_reachable"] = ping_safe(old_client())
    return st


def _run(action):
    try:
        return action()
    except MigrationConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except redis.RedisError as exc:
        raise HTTPException(status_code=503, detail=f"redis: {exc!r}")


@app.post("/api/migration/start")
def migration_start():
    return _run(engine.start)


@app.post("/api/migration/pause")
def migration_pause():
    return _run(engine.pause)


@app.post("/api/migration/step/read")
def migration_step_read():
    return _run(engine.step_read)


@app.post("/api/migration/step/write")
def migration_step_write():
    return _run(engine.step_write)


@app.post("/api/migration/step/discard")
def migration_step_discard():
    return _run(engine.step_discard)


@app.post("/api/migration/reset")
def migration_reset():
    return _run(engine.reset)


@app.post("/api/migration/fallback/disable")
def migration_disable_fallback():
    return _run(engine.disable_fallback)
