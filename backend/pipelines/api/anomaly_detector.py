def detect_anomalies(record: dict) -> list:
    anomalies = []
    if record.get("view_count", 0) == 0:
        anomalies.append("zero_views")
    if record.get("like_count", 0) < 0 or record.get("comment_count", 0) < 0:
        anomalies.append("negative_stats")
    if record.get("genre") in (None, "", "Unknown"):
        anomalies.append("unknown_genre")
    if not record.get("track_title", "").strip():
        anomalies.append("empty_title")
    if not record.get("video_id", "").strip():
        anomalies.append("missing_video_id")
    return anomalies


def log_anomalies(logger, record: dict, anomalies: list):
    logger.warning(
        f"Anomalies {anomalies} on track: '{record.get('track_title')}' "
        f"(video_id={record.get('video_id')})"
    )
