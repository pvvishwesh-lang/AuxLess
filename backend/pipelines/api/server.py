import os
import base64
import logging
import threading
from flask import Flask, request, jsonify
from backend.pipelines.run_all_users import run_for_session
from backend.pipelines.api.pubsub_publisher import publish_feedback_event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)


def _run_session_safe(session_id: str):
    """Wrapper so thread failures are logged to Cloud Logging."""
    try:
        run_for_session(session_id)
    except Exception as e:
        logger.error(f"Session {session_id} failed in background thread: {e}", exc_info=True)


@app.route("/", methods=["GET"])
def health():
    return "OK", 200


@app.route("/", methods=["POST"])
def pubsub_worker():
    envelope = request.get_json(silent=True)
    if not envelope:
        logger.warning("Received request with no JSON body")
        return "Bad Request", 400

    pubsub_message = envelope.get("message", {})
    data = pubsub_message.get("data")
    if not data:
        logger.warning("Pub/Sub message has no data field")
        return "No data", 400

    try:
        session_id = base64.b64decode(data).decode("utf-8").strip()
    except Exception as e:
        logger.error(f"Failed to decode Pub/Sub data: {e}")
        return "Invalid data encoding", 400

    logger.info(f"Received session_id: {session_id}")
    thread = threading.Thread(target=_run_session_safe, args=(session_id,), daemon=True)
    thread.start()
    return jsonify({"status": "accepted", "session_id": session_id}), 202


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
        logger.error(f"Failed to publish feedback: {e}")
        return "Internal error", 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
