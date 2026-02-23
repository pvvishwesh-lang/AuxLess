from flask import Flask, request, jsonify
from backend.pipelines.run_all_users import run_for_session,combine_gcs_files, cleanup_intermediate_files
import os
import base64
import threading
from backend.pipelines.api.firestore_client import FirestoreClient

app = Flask(__name__)

@app.route("/", methods=["GET"])
def health():
    return "OK", 200

@app.route("/", methods=["POST"])
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
        threading.Thread(target=run_for_session, args=(session_id,), daemon=True).start()
        return "Accepted", 200
    except Exception as e:
        print(f"Pipeline failed: {e}")
        return "Error", 500
    return jsonify({"status": "accepted", "session_id": session_id}), 202

@app.route("/finalize", methods=["POST"])
def finalize_session():
    print(f"Starting finalization for session {session_id}")
    data = request.get_json()
    session_id = data.get("session_id")
    if not session_id:
        return "Missing session_id", 400
    project_id = os.environ["PROJECT_ID"]
    database_id = os.environ["FIRESTORE_DATABASE"]
    bucket = "youtube-pipeline-staging-bucket"
    prefix = f"user_outputs/{session_id}/"
    fs = FirestoreClient(project_id, database_id)
    try: 
        combine_gcs_files(bucket, prefix, f"Final_Output/{session_id}_combined.csv")
        cleanup_intermediate_files(bucket, prefix)
        fs.update_session_status(session_id, "done")
        return "Finalized", 200
    except Exception as e:
        print(f"Finalization failed: {e}")
        fs.update_session_status(session_id, "error")
        return "Error", 500
        
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
