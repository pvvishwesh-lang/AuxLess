"""
monitoring_routes.py
--------------------
Flask Blueprint that powers the ML monitoring dashboard.

Register in server.py with two lines:
    from ml.api.monitoring_routes import monitoring_bp
    app.register_blueprint(monitoring_bp, url_prefix="/monitoring")

Endpoints:
    GET /monitoring/dashboard/<session_id>          <- HTML page
    GET /monitoring/session/<session_id>/state
    GET /monitoring/session/<session_id>/recommendations
    GET /monitoring/session/<session_id>/bias
    GET /monitoring/session/<session_id>/stats

All JSON endpoints return {"ok": true, "data": {...}} on success
or {"ok": false, "error": "..."} on failure.
"""

import logging
import os
import functools
import time
from pathlib import Path

from flask import Blueprint, jsonify, request, Response

from ml.recommendation.firestore_monitoring import (
    read_session_state,
    read_recommendations,
    read_bias,
    read_system_stats,
)

logger = logging.getLogger(__name__)

monitoring_bp = Blueprint("monitoring", __name__)

# ── Static file path ──────────────────────────────────────────────────────────
_STATIC_DIR = Path(__file__).parent.parent / "static"


# ── Firestore client ──────────────────────────────────────────────────────────
@functools.lru_cache(maxsize=1)
def _db():
    from google.cloud import firestore
    project = os.environ.get("GCP_PROJECT_ID", os.environ.get("PROJECT_ID", ""))
    db_name = os.environ.get("FIRESTORE_DATABASE", "auxless")
    return firestore.Client(project=project, database=db_name)


# ── In-process TTL cache ──────────────────────────────────────────────────────
_cache: dict = {}

def _cached(key: str, ttl: float, fn):
    now = time.monotonic()
    if key in _cache:
        ts, val = _cache[key]
        if now - ts < ttl:
            return val
    val = fn()
    _cache[key] = (now, val)
    return val


# ── Dashboard HTML ────────────────────────────────────────────────────────────

@monitoring_bp.route("/dashboard/<session_id>", methods=["GET"])
def dashboard(session_id):
    html_path = _STATIC_DIR / "dashboard.html"
    if not html_path.exists():
        return _error("dashboard.html not found in ml/static/", 404)
    html = html_path.read_text()
    html = html.replace("__SESSION_ID__", session_id)
    return Response(html, mimetype="text/html")


# ── 1. Session State ──────────────────────────────────────────────────────────

@monitoring_bp.route("/session/<session_id>/state", methods=["GET"])
def session_state(session_id):
    try:
        data = _cached(
            f"state:{session_id}", ttl=3.0,
            fn=lambda: read_session_state(_db(), session_id),
        )
        if "error" in data:
            return _error(data["error"], 404)
        return _ok(data)
    except Exception as exc:
        logger.exception("session_state failed")
        return _error(str(exc))


# ── 2. Recommendation Inspector ───────────────────────────────────────────────

@monitoring_bp.route("/session/<session_id>/recommendations", methods=["GET"])
def recommendations(session_id):
    try:
        cycles = int(request.args.get("cycles", 5))
        cycles = max(1, min(cycles, 20))
        data   = _cached(
            f"recs:{session_id}:{cycles}", ttl=4.0,
            fn=lambda: read_recommendations(_db(), session_id, limit=cycles),
        )
        return _ok({"cycles": data, "count": len(data)})
    except Exception as exc:
        logger.exception("recommendations failed")
        return _error(str(exc))


# ── 3. Bias Monitor ───────────────────────────────────────────────────────────

@monitoring_bp.route("/session/<session_id>/bias", methods=["GET"])
def bias(session_id):
    try:
        data = _cached(
            f"bias:{session_id}", ttl=5.0,
            fn=lambda: read_bias(_db(), session_id),
        )
        flags = []
        if data.get("popularity_flagged"):
            flags.append("popularity_bias")
        if data.get("disparity_flagged"):
            flags.append("score_disparity")
        for f in data.get("genre_flags", []):
            flags.append(f)

        data["active_flags"] = flags
        data["health"] = "ok" if not flags else ("warn" if len(flags) == 1 else "critical")
        return _ok(data)
    except Exception as exc:
        logger.exception("bias failed")
        return _error(str(exc))


# ── 4. System Stats ───────────────────────────────────────────────────────────

@monitoring_bp.route("/session/<session_id>/stats", methods=["GET"])
def system_stats(session_id):
    try:
        data = _cached(
            f"stats:{session_id}", ttl=15.0,
            fn=lambda: read_system_stats(_db(), session_id),
        )
        return _ok(data)
    except Exception as exc:
        logger.exception("system_stats failed")
        return _error(str(exc))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ok(data):
    return jsonify({"ok": True, "data": data})

def _error(msg: str, status: int = 500):
    return jsonify({"ok": False, "error": msg}), status
