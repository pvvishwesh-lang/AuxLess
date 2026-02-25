import os
import json
import logging
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)


def publish_session_ready(session_id: str):
    project_id = os.environ["PROJECT_ID"]
    topic_id   = os.environ["SESSION_READY_TOPIC"]  
    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    message = json.dumps({"session_id": session_id}).encode("utf-8")
    future  = publisher.publish(topic_path, data=message)
    msg_id  = future.result()
    logger.info(f"Published session-ready for {session_id}, msg_id={msg_id}")
