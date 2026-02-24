def detect_anomalies(record):
    anomalies = []
    if record.get('view_count', 0) == 0:
        anomalies.append('zero_views')
    if record.get('like_count', 0) < 0 or record.get('comment_count', 0) < 0:
        anomalies.append('negative_stats')
    if record.get('genre') in [None, '', 'Unknown']:
        anomalies.append('unknown_genre')
    return anomalies

def log_anomalies(logger, record, anomalies):
    logger.warning(f"Anomalies detected: {anomalies} for record: {record.get('track_title')}")