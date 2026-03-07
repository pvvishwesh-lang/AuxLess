import json
import pytest
from unittest.mock import patch, MagicMock
from backend.pipelines.api.alert_manager import (
    compute_session_anomalies,
    send_slack_alert,
)


def _make_mock_gcs(valid_rows=100, invalid_rows=2, unknown_genre_pct=10):
    def make_csv(n):
        return "header\n" + "\n".join(["row"] * n)

    genre_counts = {
        "Pop":     int(100 * (1 - unknown_genre_pct / 100)),
        "Unknown": int(100 * unknown_genre_pct / 100),
    }
    bias_json = json.dumps({"slices": {"genre": {"counts": genre_counts}}})

    def blob_side_effect(path):
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        if "combined_valid"   in path:
            mock_blob.download_as_text.return_value = make_csv(valid_rows)
        elif "combined_invalid" in path:
            mock_blob.download_as_text.return_value = make_csv(invalid_rows)
        elif "bias_metrics"   in path:
            mock_blob.download_as_text.return_value = bias_json
        return mock_blob

    mock_bucket = MagicMock()
    mock_bucket.blob.side_effect = blob_side_effect
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    return mock_client


class TestComputeSessionAnomalies:

    def test_no_anomalies_for_clean_session(self):
        mock_client = _make_mock_gcs(valid_rows=100, invalid_rows=2, unknown_genre_pct=5)
        with patch("backend.pipelines.api.alert_manager.storage.Client", return_value=mock_client):
            result = compute_session_anomalies("bucket", "sess_001")
        assert result["triggered"] is False
        assert result["anomalies"] == {}

    def test_high_invalid_rate_triggers_anomaly(self):
        mock_client = _make_mock_gcs(valid_rows=100, invalid_rows=50)
        with patch("backend.pipelines.api.alert_manager.storage.Client", return_value=mock_client):
            result = compute_session_anomalies("bucket", "sess_002")
        assert result["triggered"] is True
        assert "high_invalid_rate" in result["anomalies"]

    def test_high_unknown_genre_triggers_anomaly(self):
        mock_client = _make_mock_gcs(unknown_genre_pct=40)
        with patch("backend.pipelines.api.alert_manager.storage.Client", return_value=mock_client):
            result = compute_session_anomalies("bucket", "sess_003")
        assert result["triggered"] is True
        assert "high_unknown_genre" in result["anomalies"]

    def test_anomaly_message_contains_percentage(self):
        mock_client = _make_mock_gcs(valid_rows=100, invalid_rows=50)
        with patch("backend.pipelines.api.alert_manager.storage.Client", return_value=mock_client):
            result = compute_session_anomalies("bucket", "sess_004")
        assert "%" in result["anomalies"]["high_invalid_rate"]["message"]


class TestSendSlackAlert:

    def test_no_webhook_url_skips_silently(self):
        report = {"triggered": True, "session_id": "s1", "anomalies": {"x": {"message": "test"}}}
        with patch.dict("os.environ", {}, clear=True):
            send_slack_alert(report)  # should not raise

    def test_not_triggered_skips_request(self):
        report = {"triggered": False, "session_id": "s1", "anomalies": {}}
        with patch("requests.post") as mock_post:
            with patch.dict("os.environ", {"SLACK_WEBHOOK_URL": "https://hooks.slack.com/test"}):
                send_slack_alert(report)
        mock_post.assert_not_called()

    def test_triggered_posts_to_slack(self):
        report = {
            "triggered":  True,
            "session_id": "sess_abc123",
            "anomalies":  {"high_invalid_rate": {"message": "8% invalid records"}}
        }
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        with patch("requests.post", return_value=mock_response) as mock_post:
            with patch.dict("os.environ", {"SLACK_WEBHOOK_URL": "https://hooks.slack.com/test"}):
                send_slack_alert(report)

        mock_post.assert_called_once()
        blocks = mock_post.call_args[1]["json"]["blocks"]
        assert "sess_abc" in blocks[0]["text"]["text"]
