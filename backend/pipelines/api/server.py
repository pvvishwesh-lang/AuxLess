# server.py
from flask import Flask, request, jsonify
from backend.pipelines.run_all_users import run_for_session

app = Flask(__name__)

@app.route("/run_pipeline", methods=["POST"])
def run_pipeline():
    session_id = request.args.get("session_id")
    if not session_id:
        return jsonify({"error": "session_id required"}), 400

    try:
        run_for_session(session_id)
        return jsonify({"status": "started", "session_id": session_id}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
