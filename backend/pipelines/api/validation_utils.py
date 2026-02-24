import logging
def derive_fields(record):
    raw_millis = record.get('trackTimeMillis', 0)
    if raw_millis in (None, ''):
        track_millis = 0
    try:
        track_millis = int(float(raw_millis))
    except (TypeError, ValueError):
        logging.warning(f"Invalid trackTimeMillis: {raw_millis}, defaulting to 0")
        track_millis = 0
    record['trackTimeMillis'] = track_millis
    record['trackTimeSeconds'] = track_millis // 1000
    def to_int(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0
    view_count = to_int(record.get('view_count', 0))
    like_count = to_int(record.get('like_count', 0))
    comment_count = to_int(record.get('comment_count', 0))
    record['view_count'] = view_count
    record['like_count'] = like_count
    record['comment_count'] = comment_count
    record['like_to_view_ratio'] = like_count / view_count if view_count else 0.0
    record['comment_to_view_ratio'] = comment_count / view_count if view_count else 0.0
    return record
def validate_record(record, required_columns):
    missing = [col for col in required_columns if col not in record]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
