import os
import json
import logging
import requests
from google.cloud import storage

logger = logging.getLogger(__name__)

THRESHOLDS = {
    "invalid_record_pct": 5.0,
    "unknown_genre_pct":  30.0,
    "zero_view_pct":      20.0,
}


def _count_data_rows(bucket: object, blob_path: str) -> int:
    blob = bucket.blob(blob_path)
    if not blob.exists():
        return 0
    lines = blob.download_as_text().splitlines()
    return max(0, len(lines) - 1)  # subtract header row


def compute_session_anomalies(bucket_name: str, session_id: str) -> dict:
    client  = storage.Client()
    bucket  = client.bucket(bucket_name)
    anomalies = {}

    valid_count   = _count_data_rows(bucket, f"Final_Output/{session_id}_combined_valid.csv")
    invalid_count = _count_data_rows(bucket, f"Final_Output/{session_id}_combined_invalid.csv")
    total = valid_count + invalid_count

    if total > 0:
        invalid_pct = (invalid_count / total) * 100
        if invalid_pct > THRESHOLDS["invalid_record_pct"]:
            anomalies["high_invalid_rate"] = {
                "value":     round(invalid_pct, 2),
                "threshold": THRESHOLDS["invalid_record_pct"],
                "message":   f"{invalid_pct:.1f}% of records failed validation ({invalid_count}/{total})"
            }

    bias_blob = bucket.blob(f"Final_Output/{session_id}_bias_metrics.json")
    if bias_blob.exists():
        bias = json.loads(bias_blob.download_as_text())
        genre_data = bias.get("slices", {}).get("genre", {})
        genre_counts = genre_data.get("counts", {})
        total_genre  = sum(genre_counts.values())
        unknown_count = genre_counts.get("Unknown", 0)
        if total_genre > 0:
            unknown_pct = (unknown_count / total_genre) * 100
            if unknown_pct > THRESHOLDS["unknown_genre_pct"]:
                anomalies["high_unknown_genre"] = {
                    "value":     round(unknown_pct, 2),
                    "threshold": THRESHOLDS["unknown_genre_pct"],
                    "message":   f"{unknown_pct:.1f}% of tracks have unknown genre"
                }

    return {
        "session_id": session_id,
        "anomalies":  anomalies,
        "triggered":  len(anomalies) > 0
    }


def send_slack_alert(report: dict):
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not set — Slack alert skipped.")
        return
    if not report.get("triggered"):
        return

    session_id = report["session_id"]
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"AuxLess Pipeline Alert — Session {session_id[:8]}"
            }
        }
    ]
    for key, detail in report["anomalies"].items():
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*{key}*\n{detail['message']}"}
        })

    try:
        res = requests.post(webhook_url, json={"blocks": blocks}, timeout=5)
        res.raise_for_status()
        logger.info(f"Slack alert sent for session {session_id}")
    except Exception as e:
        logger.error(f"Slack alert failed: {e}")


def run_anomaly_checks_and_alert(bucket_name: str, session_id: str) -> dict:
    report = compute_session_anomalies(bucket_name, session_id)
    if report["triggered"]:
        logger.warning(f"Anomalies for {session_id}: {list(report['anomalies'].keys())}")
        send_slack_alert(report)
    else:
        logger.info(f"No anomalies for session {session_id}")
    return report
