import os
import json
import time
import base64
import logging
import functions_framework
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)


@functions_framework.cloud_event
def trigger_streaming_pipeline(cloud_event):
    try:
        data       = cloud_event.data["message"]["data"]
        payload    = json.loads(base64.b64decode(data).decode("utf-8"))
        session_id = payload["session_id"]
    except Exception as e:
        logger.error(f"Failed to decode Pub/Sub message: {e}")
        return

    logger.info(f"Launching streaming pipeline for session: {session_id}")

    project_id = os.environ["PROJECT_ID"]
    region     = os.environ.get("REGION", "us-central1")
    bucket     = os.environ["BUCKET"]
    template   = os.environ["STREAMING_TEMPLATE_PATH"]

    dataflow = build("dataflow", "v1b3")

    body = {
        "launchParameter": {
            "jobName": f"feedback-streaming-{session_id[:10]}-{int(time.time())}",
            "parameters": {
                "session_id":         session_id,
                "input_subscription": f"projects/{project_id}/subscriptions/feedback-events-sub",
                "firestore_project":  project_id,
                "firestore_database": os.environ["FIRESTORE_DATABASE"],
                "bucket":             bucket,
            },
            "environment": {
                "tempLocation":      f"gs://{bucket}/temp",
                "stagingLocation":   f"gs://{bucket}/staging",
                "sdkContainerImage": f"{os.environ.get('SDK_CONTAINER_IMAGE', '')}",
                "machineType": "e2-standard-2",
            },
            "containerSpecGcsPath": template
        }
    }

    if not body["launchParameter"]["environment"]["sdkContainerImage"]:
        del body["launchParameter"]["environment"]["sdkContainerImage"]

    for attempt in range(3):
        try:
            response = (
                dataflow.projects()
                .locations()
                .flexTemplates()
                .launch(projectId=project_id, location=region, body=body)
                .execute()
            )
            logger.info(f"Dataflow streaming job launched: {response['job']['id']}")
            break
        except Exception as e:
            if "ZONE_RESOURCE_POOL_EXHAUSTED" in str(e) and attempt < 2:
                logger.warning(f"Zone exhausted, retrying in 30s (attempt {attempt+1})")
                time.sleep(30)
            elif "already an active job" in str(e):
                logger.info(f"Streaming job already running for session {session_id}, skipping.")
                break
            else:
                logger.error(f"Failed to launch streaming job: {e}")
                raise
