import os
import requests

CLOUD_RUN_URL = os.environ.get(
    "CLOUD_RUN_URL",
    "https://auxless-610684648990.us-east1.run.app/run_pipeline"
)

def firestore_session_trigger(event, context):
    value = event.get("value")
    if not value:
        return

    fields = value.get("fields", {})
    session_id = value["name"].split("/")[-1]

    status = fields.get("status", {}).get("stringValue")

    users = (
        fields.get("users", {})
              .get("arrayValue", {})
              .get("values", [])
    )

    print(f"Session {session_id} | status={status} | users={len(users)}")

    if status != "pending":
        return

    if not users:
        return

    url = f"{CLOUD_RUN_URL}?session_id={session_id}"
    resp = requests.post(url, timeout=10)

    print("Triggered Cloud Run:", resp.status_code)
