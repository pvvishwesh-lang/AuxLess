import apache_beam as beam
from backend.pipelines.api.ReadFromAPI import ReadFromAPI
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
        self._api = None

    def setup(self):
        self._api = ReadFromAPI(self.access_token)
        if hasattr(self._api, "setup"):
            self._api.setup()


    def process(self, element):
        for record in self._api.process(element):
            record = derive_fields(record)
            validate_record(record, COLUMNS)
            anomalies = detect_anomalies(record)
            if anomalies:
                log_anomalies(self.logger, record, anomalies)
            yield record
