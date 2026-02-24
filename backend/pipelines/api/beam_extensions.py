import apache_beam as beam
from backend.pipelines.api.validation_utils import derive_fields, validate_record
from backend.pipelines.api.anomaly_detector import detect_anomalies, log_anomalies
from backend.pipelines.api.structured_logger import get_logger

COLUMNS = [
    'playlist_name', 'track_title', 'artist_name', 'video_id', 'genre',
    'country','collection_name','collection_id','trackTimeMillis',
    'trackTimeSeconds',
    'view_count','like_count','comment_count',
    'like_to_view_ratio','comment_to_view_ratio'
]
class ValidatingDoFn(beam.DoFn):
    def __init__(self, access_token, session_id, user_id):
        self.access_token = access_token
        self.session_id = session_id
        self.user_id = user_id
        self.logger = get_logger(session_id, user_id)
    def process(self, element):
        if element is None:
            return
        try:
            record = derive_fields(element)
            validate_record(record, COLUMNS, self.logger)
            anomalies = detect_anomalies(record)
            if anomalies:
                log_anomalies(self.logger, record, anomalies)
            yield record  
        except Exception as e:
            self.logger.error(
                "invalid_record_schema",
                extra={
                    "video_id": element.get("video_id"),
                    "error": str(e),
                },
            )
            yield beam.pvalue.TaggedOutput(
                "invalid_records",
                {
                    "error": str(e),
                    "record": element,
                },
            )
