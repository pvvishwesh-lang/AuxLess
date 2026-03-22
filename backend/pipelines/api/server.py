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
from ml.ml_trigger import _run_ml_session

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
else:
    for h in logger.handlers:
        h.setStream(sys.stderr)

app = Flask(__name__)

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
        _run_ml_session(session_id)
    except Exception as e:
        logging.getLogger(__name__).error(
            f"ML session {session_id} failed in background thread: {e}",
            exc_info=True
        )

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
    log.info(f"Session {session_id} ending — syncing to BQ and cleaning up")
    try:
        project_id  = os.environ["PROJECT_ID"]
        database_id = os.environ["FIRESTORE_DATABASE"]
        fs = FirestoreClient(project_id, database_id)
        users = fs.get_session_users(session_id)
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
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to publish feedback: {e}")
        return "Internal error", 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
