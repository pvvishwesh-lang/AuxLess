def derive_fields(record):
    record['trackTimeSeconds'] = record.get('trackTimeMillis', 0) // 1000
    view_count = record.get('view_count', 0)
    record['like_to_view_ratio'] = round(record.get('like_count', 0) / max(view_count, 1), 4)
    record['comment_to_view_ratio'] = round(record.get('comment_count', 0) / max(view_count, 1), 4)
    return record

def validate_record(record, required_columns):
    missing = [col for col in required_columns if col not in record]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")