# server.py
from flask import Flask, request, jsonify
from backend.pipelines.run_all_users import run_for_session
import threading

app = Flask(__name__)

@app.route("/run_pipeline", methods=["POST",'GET'])
def run_pipeline():
    session_id = request.args.get("session_id")
    if not session_id:
        return jsonify({"error": "session_id required"}), 400
    thread = threading.Thread(target=run_for_session, args=(session_id,), daemon=True).start()
    return jsonify({"status": "accepted", "session_id": session_id}), 202

    


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)


