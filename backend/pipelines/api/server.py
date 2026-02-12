from flask import Flask, request, jsonify
from backend.pipelines.run_all_users import run_for_session
import os
import base64
import threading

app = Flask(__name__)

@app.route("/", methods=["POST",'GET'])
def pubsub_worker():
    envelope = request.get_json()
    if not envelope:
        return "Bad Request", 400
    pubsub_message = envelope.get("message", {})
    data = pubsub_message.get("data")
    if not data:
        return "No data", 400
    session_id = base64.b64decode(data).decode("utf-8")
    print(f"Received session {session_id}")
    try:
        run_for_session(session_id) 
        return "OK", 200
    except Exception as e:
        print(f"Pipeline failed: {e}")
        return "Error", 500
    return jsonify({"status": "accepted", "session_id": session_id}), 202

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
