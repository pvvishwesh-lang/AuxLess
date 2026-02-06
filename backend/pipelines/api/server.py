from flask import Flask, request, jsonify
from Api_Puller import run_pipeline_for_user

app = Flask(__name__)

GCS_PREFIX = "user_outputs/"

@app.route("/run_pipeline", methods=["POST"])
def run_pipeline():
    data = request.get_json()
    user_id = data.get("user_id")
    refresh_token = data.get("refresh_token")
    if not user_id or not refresh_token:
        return jsonify({"error": "Missing user_id or refresh_token"}), 400

    run_pipeline_for_user(user_id, refresh_token, GCS_PREFIX)
    return jsonify({"status": "pipeline_started"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
