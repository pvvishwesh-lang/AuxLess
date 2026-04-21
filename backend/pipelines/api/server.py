import os
import sys
import base64
import json
import logging
import threading
from flask import Flask, request, jsonify
from backend.pipelines.run_all_users import run_for_session
from backend.pipelines.api.pubsub_publisher import publish_feedback_event
from backend.pipelines.api.firestore_client import FirestoreClient
from backend.pipelines.api.bq_sync import sync_session_to_bigquery
from ml.api.play_event_writer import write_play_event_from_feedback
from ml.api.monitoring_routes import monitoring_bp
import time
from flask_cors import CORS


logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)

app = Flask(__name__)
CORS(app)
app.register_blueprint(monitoring_bp, url_prefix="/monitoring")


def _run_session_safe(session_id: str):
    try:
        run_for_session(session_id)
    except Exception as e:
        logging.getLogger(__name__).error(
            f"Session {session_id} failed in background thread: {e}",
            exc_info=True
        )


def _run_ml_session_safe(session_id: str):
    try:
        from ml.ml_trigger import _run_ml_session
        _run_ml_session(session_id)
    except Exception as e:
        logging.getLogger(__name__).error(f"ML session {session_id} failed in background thread: {e}", exc_info=True)


def _drain_streaming_job(project_id: str, session_id: str):
    from googleapiclient.discovery import build
    log = logging.getLogger(__name__)
    region = os.environ.get("DATAFLOW_REGION", "us-central1")
    dataflow = build("dataflow", "v1b3")
    try:
        jobs = dataflow.projects().locations().jobs().list(
            projectId=project_id,
            location=region
        ).execute()
        for job in jobs.get("jobs", []):
            log.info(f"Checking job: {job.get('name')} state: {job.get('currentState')}")
            if session_id[:10] in job.get("name", "") and job.get("currentState") == "JOB_STATE_RUNNING":
                dataflow.projects().locations().jobs().update(
                    projectId=project_id,
                    location=region,
                    jobId=job["id"],
                    body={"requestedState": "JOB_STATE_DRAINED"}
                ).execute()
                log.info(f"Draining streaming job {job['id']} ({job['name']})")
                for _ in range(60):
                    status = dataflow.projects().locations().jobs().get(
                        projectId=project_id,
                        location=region,
                        jobId=job["id"]
                    ).execute()
                    state = status.get("currentState", "")
                    if state in ("JOB_STATE_DRAINED", "JOB_STATE_CANCELLED", "JOB_STATE_DONE"):
                        log.info(f"Streaming job {job['id']} drained successfully")
                        return True
                    time.sleep(5)
                log.warning(f"Streaming job {job['id']} drain timed out")
                return True
        log.warning(f"No active streaming job found for session {session_id}")
        return True
    except Exception as e:
        log.error(f"Failed to drain streaming job: {e}")
        return False


@app.route("/", methods=["GET"])
def health():
    return "OK", 200


@app.route("/", methods=["POST"])
def pubsub_worker():
    envelope = request.get_json(silent=True)
    if not envelope:
        logging.getLogger(__name__).warning("Received request with no JSON body")
        return "Bad Request", 400

    pubsub_message = envelope.get("message", {})
    data = pubsub_message.get("data")
    if not data:
        logging.getLogger(__name__).warning("Pub/Sub message has no data field")
        return "No data", 400

    try:
        session_id = base64.b64decode(data).decode("utf-8").strip()
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to decode Pub/Sub data: {e}")
        return "Invalid data encoding", 400

    logging.getLogger(__name__).info(f"Batch pipeline triggered for session: {session_id}")
    thread = threading.Thread(
        target=_run_session_safe,
        args=(session_id,),
        daemon=True
    )
    thread.start()
    return jsonify({"status": "accepted", "session_id": session_id}), 202


@app.route("/ml", methods=["POST"])
def ml_worker():
    envelope = request.get_json(silent=True)
    if not envelope:
        logging.getLogger(__name__).warning("ML endpoint received request with no JSON body")
        return "Bad Request", 400
    pubsub_message = envelope.get("message", {})
    data = pubsub_message.get("data")
    if not data:
        logging.getLogger(__name__).warning("ML Pub/Sub message has no data field")
        return "No data", 400
    try:
        payload    = json.loads(base64.b64decode(data).decode("utf-8"))
        session_id = payload["session_id"]
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to decode ML Pub/Sub data: {e}")
        return "Invalid data encoding", 400
    logging.getLogger(__name__).info(f"ML pipeline triggered for session: {session_id}")
    thread = threading.Thread(
        target=_run_ml_session_safe,
        args=(session_id,),
        daemon=True
    )
    thread.start()
    return jsonify({"status": "accepted", "session_id": session_id}), 202


@app.route("/end_session", methods=["POST"])
def end_session():
    data = request.get_json(silent=True)
    if not data or "session_id" not in data:
        return "Missing session_id", 400
    session_id = data["session_id"]
    log = logging.getLogger(__name__)
    log.info(f"Session {session_id} ending — draining pipeline, syncing to BQ")
    try:
        project_id  = os.environ["PROJECT_ID"]
        database_id = os.environ["FIRESTORE_DATABASE"]
        fs = FirestoreClient(project_id, database_id)
        _drain_streaming_job(project_id, session_id)
        users    = fs.get_session_users(session_id)
        user_ids = [uid for uid, _ in users] if users else []
        sync_session_to_bigquery(
            session_id=session_id,
            project_id=project_id,
            database_id=database_id,
            user_ids=user_ids
        )
        log.info(f"BQ sync complete for session {session_id}")
        fs.update_session_status(session_id, "ended")
        log.info(f"Session {session_id} marked as ended")
        return jsonify({"status": "ended", "session_id": session_id}), 200
    except Exception as e:
        log.error(f"End session failed: {e}", exc_info=True)
        return "Internal error", 500


@app.route("/feedback", methods=["POST"])
def feedback():
    data = request.get_json(silent=True)
    if not data:
        return "Bad Request", 400
    required = {"session_id", "user_id", "video_id", "action"}
    if not required.issubset(data.keys()):
        return "Missing fields", 400
    if data["action"] not in ("like", "dislike", "skip", "replay"):
        return "Invalid action", 400
    try:
        publish_feedback_event(data)
        threading.Thread(
            target=write_play_event_from_feedback,
            args=(
                data["session_id"],
                data["video_id"],
                data["action"],
                float(data.get("play_duration_sec", 0.0)),
            ),
            daemon=True,
        ).start()
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to publish feedback: {e}")
        return "Internal error", 500


@app.route("/start_session", methods=["POST"])
def start_session():
    data = request.get_json(silent=True)
    if not data or "session_id" not in data:
        return "Missing session_id", 400
    session_id = data["session_id"]
    log = logging.getLogger(__name__)
    try:
        project_id  = os.environ["PROJECT_ID"]
        database_id = os.environ["FIRESTORE_DATABASE"]
        fs = FirestoreClient(project_id, database_id)
        fs.update_session_status(session_id, "running")
        log.info(f"Session {session_id} set to running — Firestore trigger will launch pipeline")
    except Exception as e:
        log.error(f"Failed to update session status: {e}")
        return "Internal error", 500
    return jsonify({"status": "accepted", "session_id": session_id}), 202


# ── CT/CM Endpoints ───────────────────────────────────────────────────────────

@app.route("/retrain", methods=["POST"])
def retrain():
    """
    Continuous Training (CT) trigger endpoint.
    Called by Cloud Scheduler every 3 days.
    Runs drift detection first, then GRU retraining.
    Deploys new model to GCS only if val_loss improves.
    """
    log = logging.getLogger(__name__)
    log.info("Retraining pipeline triggered")

    def _run_retrain():
        try:
            from ml.ct_cm.drift_detector import run_drift_detection
            drift_result = run_drift_detection()
            log.info(
                f"Drift detection complete: KL={drift_result.get('kl_divergence', 0):.4f}, "
                f"drift_detected={drift_result.get('drift_detected', False)}"
            )
            from ml.ct_cm.retrain import run_retraining
            retrain_result = run_retraining()
            log.info(
                f"Retraining complete: deployed={retrain_result.get('deployed', False)}, "
                f"val_loss={retrain_result.get('new_val_loss', 'N/A')}"
            )
        except Exception as e:
            log.error(f"Retraining pipeline failed: {e}", exc_info=True)

    thread = threading.Thread(target=_run_retrain, daemon=True)
    thread.start()

    return jsonify({
        "status":  "accepted",
        "message": "CT/CM pipeline started - drift detection + GRU retraining running in background",
    }), 202


@app.route("/monitoring/health", methods=["GET"])
def monitoring_health():
    """
    Health endpoint for Cloud Monitoring uptime checks.
    Checks Firestore connectivity and returns system status.
    """
    log = logging.getLogger(__name__)
    try:
        from google.cloud import firestore as _fs
        project_id  = os.environ.get("PROJECT_ID",        "auxless-music-recommender")
        database_id = os.environ.get("FIRESTORE_DATABASE", "auxless")
        db          = _fs.Client(project=project_id, database=database_id)
        db.collection("sessions").limit(1).get()
        return jsonify({
            "status":    "healthy",
            "firestore": "connected",
            "timestamp": time.time(),
        }), 200
    except Exception as e:
        log.error(f"Health check failed: {e}")
        return jsonify({
            "status":    "unhealthy",
            "error":     str(e),
            "timestamp": time.time(),
        }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))