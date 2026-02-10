import os
import requests
import functions_framework
from google.events.cloud.firestore_v1 import DocumentEventData
from google.auth.transport.requests import Request
from google.oauth2 import id_token

CLOUD_RUN_URL = os.environ.get("CLOUD_RUN_URL","https://auxless-610684648990.europe-west1.run.app/run_pipeline")

@functions_framework.cloud_event
def firestore_session_trigger(cloud_event):
    firestore_payload = DocumentEventData.deserialize(cloud_event.data)    
    doc_snapshot = firestore_payload.value
    if not doc_snapshot:
        return
    session_id = doc_snapshot.name.split("/")[-1]
    fields = doc_snapshot.fields
    status = fields.get("status").string_value if "status" in fields else None
    users_list = fields.get("users").array_value.values if "users" in fields else []
    print(f"Session {session_id} | Status: {status} | Users count: {len(users_list)}")
    if status != "pending" or not users_list:
        return
    try:
        auth_req = Request()
        token = id_token.fetch_id_token(auth_req, CLOUD_RUN_URL)
        headers = {"Authorization": f"Bearer {token}"}
        url = f"{CLOUD_RUN_URL}?session_id={session_id}"
        resp = requests.post(url, headers=headers, timeout=10)
        print(f"Trigger sent. Result: {resp.status_code}")
    except Exception as e:
        print(f"Trigger failed: {e}")
