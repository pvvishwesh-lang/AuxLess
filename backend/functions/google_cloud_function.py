import os
import requests
from google.cloud import firestore

CLOUD_RUN_URL = os.environ.get("https://auxless-610684648990.us-east1.run.app/run_pipeline")  

def firestore_session_trigger(event, context):
    print("Event ID:", context.event_id)
    print("Event type:", context.event_type)
    resource_string = context.resource
    print("Resource:", resource_string)
    session_data = event.get("value", {}).get("fields", {})
    if not session_data:
        print("No data in the event.")
        return

    users_field = session_data.get("users", {}).get("arrayValue", {}).get("values", [])
    session_id = event.get("value", {}).get("name", "").split("/")[-1]

    users_list = []
    for u in users_field:
        u_dict = {}
        for k,v in u.get("mapValue", {}).get("fields", {}).items():
            u_dict[k] = v.get("stringValue")
        users_list.append(u_dict)

    if len(users_list) == 0:
        print("No users yet.")
        return

    payload = {
        "session_id": session_id,
        "users": users_list
    }

    response = requests.post(CLOUD_RUN_URL, json=payload)
    print("Pipeline trigger response:", response.status_code, response.text)
