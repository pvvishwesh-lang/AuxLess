"""
monitoring_routes.py
--------------------
FastAPI router that powers the monitoring dashboard.
Register in your main Cloud Run app entrypoint:

    from ml.api.monitoring_routes import router as monitoring_router
    app.include_router(monitoring_router, prefix="/monitoring")

Then the dashboard is served at:
    GET /monitoring/dashboard/{session_id}     <- HTML page
    GET /monitoring/session/{session_id}/state
    GET /monitoring/session/{session_id}/recommendations
    GET /monitoring/session/{session_id}/bias
    GET /monitoring/session/{session_id}/stats

All JSON endpoints return {"ok": true, "data": {...}} on success
or {"ok": false, "error": "..."} on failure — consistent shape
makes the dashboard JS simple.
"""

from __future__ import annotations

import logging
import os
import functools
import time
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from google.cloud import firestore

from ml.recommendation.firestore_monitoring import (
    read_session_state,
    read_recommendations,
    read_bias,
    read_system_stats,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["monitoring"])

# ---------------------------------------------------------------------------
# Firestore client — reuse the existing singleton pattern from your codebase
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=1)
def _db() -> firestore.Client:
    project = os.environ.get("GCP_PROJECT_ID", "your-gcp-project")
    db_name = os.environ.get("FIRESTORE_DB", "auxless")
    return firestore.Client(project=project, database=db_name)


# ---------------------------------------------------------------------------
# Simple in-process cache — avoids hammering Firestore on every poll tick.
# Stats endpoint is heavier (BigQuery), so it gets a longer TTL.
# ---------------------------------------------------------------------------

_cache: dict[str, tuple[float, Any]] = {}

def _cached(key: str, ttl: float, fn):
    now = time.monotonic()
    if key in _cache:
        ts, val = _cache[key]
        if now - ts < ttl:
            return val
    val = fn()
    _cache[key] = (now, val)
    return val


# ---------------------------------------------------------------------------
# Dashboard HTML — serves the static file from ml/static/dashboard.html
# ---------------------------------------------------------------------------

_STATIC_DIR = Path(__file__).parent.parent / "static"

@router.get("/dashboard/{session_id}", response_class=HTMLResponse)
async def dashboard(session_id: str):
    html_path = _STATIC_DIR / "dashboard.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="dashboard.html not found in ml/static/")
    html = html_path.read_text()
    # Inject the session_id so the JS knows which session to poll
    html = html.replace("__SESSION_ID__", session_id)
    return HTMLResponse(content=html)


# ---------------------------------------------------------------------------
# 1. Session State
# ---------------------------------------------------------------------------

@router.get("/session/{session_id}/state")
async def session_state(session_id: str):
    """
    Returns:
      phase, weights (cbf/cf/gru), songs_played,
      feedback_counts (like/skip/replay/dislike/neutral),
      avg_play_duration_sec, user_count, status
    """
    try:
        data = _cached(
            f"state:{session_id}", ttl=3.0,
            fn=lambda: read_session_state(_db(), session_id)
        )
        if "error" in data:
            return _error(data["error"], 404)
        return _ok(data)
    except Exception as exc:
        logger.exception("session_state failed")
        return _error(str(exc))


# ---------------------------------------------------------------------------
# 2. Recommendation Inspector
# ---------------------------------------------------------------------------

@router.get("/session/{session_id}/recommendations")
async def recommendations(
    session_id: str,
    cycles: int = Query(default=5, ge=1, le=20, description="How many past regen cycles to return"),
):
    """
    Returns last N recommendation cycles.
    Each cycle contains:
      phase, weights, ts_ms,
      songs: [{rank, title, artist, genre, cbf_score, cf_score, gru_score, final_score}]
    """
    try:
        data = _cached(
            f"recs:{session_id}:{cycles}", ttl=4.0,
            fn=lambda: read_recommendations(_db(), session_id, limit=cycles)
        )
        return _ok({"cycles": data, "count": len(data)})
    except Exception as exc:
        logger.exception("recommendations failed")
        return _error(str(exc))


# ---------------------------------------------------------------------------
# 3. Bias Monitor
# ---------------------------------------------------------------------------

@router.get("/session/{session_id}/bias")
async def bias(session_id: str):
    """
    Returns the latest bias snapshot:
      genre_distribution, genre_flags,
      popularity_ratio, popularity_flagged,
      score_disparity, disparity_flagged,
      diversity_score (Shannon entropy),
      mitigation_suggestions
    """
    try:
        data = _cached(
            f"bias:{session_id}", ttl=5.0,
            fn=lambda: read_bias(_db(), session_id)
        )
        # Compute overall health signal for the UI
        flags = []
        if data.get("popularity_flagged"):
            flags.append("popularity_bias")
        if data.get("disparity_flagged"):
            flags.append("score_disparity")
        if data.get("genre_flags"):
            flags.extend(data["genre_flags"])

        data["active_flags"] = flags
        data["health"] = "ok" if not flags else ("warn" if len(flags) == 1 else "critical")
        return _ok(data)
    except Exception as exc:
        logger.exception("bias failed")
        return _error(str(exc))


# ---------------------------------------------------------------------------
# 4. System Stats
# ---------------------------------------------------------------------------

@router.get("/session/{session_id}/stats")
async def system_stats(session_id: str):
    """
    Heavier endpoint — cached for 15s.
    Returns:
      genre_distribution, popularity_histogram (10 buckets),
      diversity_trend (entropy per regen cycle),
      avg_play_duration_sec, avg_feedback_score,
      feedback_score_interpretation, total_plays
    """
    try:
        data = _cached(
            f"stats:{session_id}", ttl=15.0,
            fn=lambda: read_system_stats(_db(), session_id)
        )
        return _ok(data)
    except Exception as exc:
        logger.exception("system_stats failed")
        return _error(str(exc))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ok(data: Any) -> JSONResponse:
    return JSONResponse({"ok": True, "data": data})

def _error(msg: str, status: int = 500) -> JSONResponse:
    return JSONResponse({"ok": False, "error": msg}, status_code=status)
