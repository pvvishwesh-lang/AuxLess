from flask import Flask, request, jsonify
from backend.pipelines.api.run_all_users import main as run_all_users

app = Flask(__name__)

@app.route("/run_pipeline", methods=["POST"])
def run_pipeline():
    try:
        run_all_users()
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
