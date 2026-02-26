import json
import logging
import apache_beam as beam
from google.cloud import firestore

logger = logging.getLogger(__name__)
WEIGHTS = {
    "like":    +2.0,
    "dislike": -2.0,
    "skip":    -0.5,
    "replay":  +1.5,
}

class ParseFeedbackFn(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element.decode("utf-8"))
            required = {"session_id", "user_id", "video_id", "action"}
            if not required.issubset(record.keys()):
                logger.warning(f"Incomplete feedback record: {record}")
                return
            yield record
        except Exception as e:
            logger.error(f"Failed to parse feedback message: {e}")

class FilterBySessionFn(beam.DoFn):
    def __init__(self, session_id: str):
        self.session_id = session_id
    def process(self, element):
        if element.get("session_id") == self.session_id:
            yield element
        else:
            logger.debug(
                f"Skipping feedback for session {element.get('session_id')}, "
                f"expected {self.session_id}"
            )

class ScoreFeedbackFn(beam.DoFn):
    def process(self, element):
        action = element.get("action", "").lower()
        delta  = WEIGHTS.get(action, 0.0)
        element["score_delta"] = delta
        logger.info(
            f"Scored action='{action}' on video={element['video_id']} "
            f"delta={delta}"
        )
        yield element

class UpdateFirestoreFn(beam.DoFn):
    def __init__(self, project_id: str, database_id: str, session_id: str):
        self.project_id  = project_id
        self.database_id = database_id
        self.session_id  = session_id
        self.db          = None  
    def setup(self):
        self.db = firestore.Client(
            project=self.project_id,
            database=self.database_id
        )
    def process(self, element):
        video_id    = element["video_id"]
        action      = element["action"]
        score_delta = element["score_delta"]
        track_ref = (
            self.db
            .collection("sessions")
            .document(self.session_id)
            .collection("tracks")
            .document(video_id)
        )
        count_field = f"{action}_count"
        track_ref.set(
            {
                "score":       firestore.transforms.INCREMENT(score_delta),
                count_field:   firestore.transforms.INCREMENT(1),
                "last_updated": firestore.SERVER_TIMESTAMP,
                "video_id":    video_id,
            },
            merge=True  
        )

        logger.info(
            f"Updated Firestore: session={self.session_id} "
            f"video={video_id} action={action} delta={score_delta}"
        )
        yield element  
