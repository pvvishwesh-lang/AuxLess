import pytest
from backend.pipelines.api.anomaly_detector import detect_anomalies, log_anomalies
from unittest.mock import MagicMock


class TestDetectAnomalies:

    def test_clean_record_no_anomalies(self, derived_record):
        assert detect_anomalies(derived_record) == []

    def test_zero_views_flagged(self, derived_record):
        derived_record["view_count"] = 0
        assert "zero_views" in detect_anomalies(derived_record)

    def test_negative_like_count_flagged(self, derived_record):
        derived_record["like_count"] = -1
        assert "negative_stats" in detect_anomalies(derived_record)

    def test_negative_comment_count_flagged(self, derived_record):
        derived_record["comment_count"] = -1
        assert "negative_stats" in detect_anomalies(derived_record)

    def test_unknown_genre_flagged(self, derived_record):
        derived_record["genre"] = "Unknown"
        assert "unknown_genre" in detect_anomalies(derived_record)

    def test_none_genre_flagged(self, derived_record):
        derived_record["genre"] = None
        assert "unknown_genre" in detect_anomalies(derived_record)

    def test_empty_genre_flagged(self, derived_record):
        derived_record["genre"] = ""
        assert "unknown_genre" in detect_anomalies(derived_record)

    def test_empty_title_flagged(self, derived_record):
        derived_record["track_title"] = "   "
        assert "empty_title" in detect_anomalies(derived_record)

    def test_missing_video_id_flagged(self, derived_record):
        derived_record["video_id"] = ""
        assert "missing_video_id" in detect_anomalies(derived_record)

    def test_multiple_anomalies_detected(self, derived_record):
        derived_record["view_count"] = 0
        derived_record["genre"]      = "Unknown"
        anomalies = detect_anomalies(derived_record)
        assert "zero_views"    in anomalies
        assert "unknown_genre" in anomalies


class TestLogAnomalies:

    def test_log_anomalies_calls_logger(self, derived_record):
        mock_logger = MagicMock()
        log_anomalies(mock_logger, derived_record, ["zero_views"])
        mock_logger.warning.assert_called_once()
        call_args = mock_logger.warning.call_args[0][0]
        assert "zero_views"      in call_args
        assert "Blinding Lights" in call_args
