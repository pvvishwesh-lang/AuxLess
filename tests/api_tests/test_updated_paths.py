import pytest
from unittest.mock import patch, MagicMock
import pandas as pd


class TestBiasMitigationPaths:
    @patch("backend.pipelines.api.bias_mitigation.storage.Client")
    def test_default_paths_use_session_structure(self, mock_storage):
        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket

        csv_content = "genre,country,value\nPop,US,1\nRock,US,2\nPop,UK,3\n"
        mock_bucket.blob.return_value.download_as_text.return_value = csv_content
        mock_bucket.blob.return_value.upload_from_string = MagicMock()

        from backend.pipelines.api.bias_mitigation import run_bias_mitigation
        run_bias_mitigation(
            bucket_name="test-bucket",
            session_id="abc123",
            slice_cols=["genre"]
        )

        blob_calls = [call[0][0] for call in mock_bucket.blob.call_args_list]
        assert any("sessions/abc123/combined/valid/" in b for b in blob_calls)
        assert any("sessions/abc123/bias_mitigation/" in b for b in blob_calls)

    @patch("backend.pipelines.api.bias_mitigation.storage.Client")
    def test_custom_paths_override_defaults(self, mock_storage):
        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket

        csv_content = "genre,value\nPop,1\nRock,2\n"
        mock_bucket.blob.return_value.download_as_text.return_value = csv_content
        mock_bucket.blob.return_value.upload_from_string = MagicMock()

        from backend.pipelines.api.bias_mitigation import run_bias_mitigation
        run_bias_mitigation(
            bucket_name="test-bucket",
            session_id="abc123",
            slice_cols=["genre"],
            input_path="custom/input.csv",
            output_csv_path="custom/output.csv",
            output_report_path="custom/report.json"
        )

        blob_calls = [call[0][0] for call in mock_bucket.blob.call_args_list]
        assert "custom/input.csv" in blob_calls
        assert "custom/output.csv" in blob_calls
        assert "custom/report.json" in blob_calls


class TestSchemaValidatorPaths:
    @patch("backend.pipelines.api.schema_validator.storage.Client")
    def test_default_path_uses_session_structure(self, mock_storage):
        csv_content = (
            "playlist_name,track_title,artist_name,video_id,genre,country,"
            "collection_name,collection_id,trackTimeMillis,trackTimeSeconds,"
            "view_count,like_count,comment_count,like_to_view_ratio,comment_to_view_ratio\n"
            "MyPlaylist,Song1,Artist1,vid1,Pop,US,Album1,1,200000,200,1000,100,10,0.1,0.01\n"
        )
        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value.download_as_text.return_value = csv_content

        from backend.pipelines.api.schema_validator import run_schema_validation
        result = run_schema_validation(
            bucket_name="test-bucket",
            session_id="abc123"
        )

        blob_path = mock_bucket.blob.call_args[0][0]
        assert "sessions/abc123/combined/valid/" in blob_path
        assert result["session_id"] == "abc123"

    @patch("backend.pipelines.api.schema_validator.storage.Client")
    def test_custom_path_overrides_default(self, mock_storage):
        csv_content = (
            "playlist_name,track_title,artist_name,video_id,genre,country,"
            "collection_name,collection_id,trackTimeMillis,trackTimeSeconds,"
            "view_count,like_count,comment_count,like_to_view_ratio,comment_to_view_ratio\n"
            "MyPlaylist,Song1,Artist1,vid1,Pop,US,Album1,1,200000,200,1000,100,10,0.1,0.01\n"
        )
        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value.download_as_text.return_value = csv_content

        from backend.pipelines.api.schema_validator import run_schema_validation
        result = run_schema_validation(
            bucket_name="test-bucket",
            session_id="abc123",
            input_path="custom/valid.csv"
        )

        blob_path = mock_bucket.blob.call_args[0][0]
        assert blob_path == "custom/valid.csv"


class TestAlertManagerPaths:
    @patch("backend.pipelines.api.alert_manager.storage.Client")
    def test_default_paths_use_session_structure(self, mock_storage):
        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket

        valid_blob = MagicMock()
        valid_blob.exists.return_value = True
        valid_blob.download_as_text.return_value = "header\nrow1\nrow2\n"

        invalid_blob = MagicMock()
        invalid_blob.exists.return_value = True
        invalid_blob.download_as_text.return_value = "header\nrow1\n"

        bias_blob = MagicMock()
        bias_blob.exists.return_value = False

        def side_effect(path):
            if "valid" in path and "invalid" not in path:
                return valid_blob
            elif "invalid" in path:
                return invalid_blob
            elif "bias" in path:
                return bias_blob
            return MagicMock(exists=MagicMock(return_value=False))

        mock_bucket.blob.side_effect = side_effect

        from backend.pipelines.api.alert_manager import compute_session_anomalies
        result = compute_session_anomalies(
            bucket_name="test-bucket",
            session_id="abc123"
        )

        assert result["session_id"] == "abc123"
        assert isinstance(result["anomalies"], dict)

    @patch("backend.pipelines.api.alert_manager.storage.Client")
    def test_custom_paths_override_defaults(self, mock_storage):
        import json as _json

        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket

        bias_json = _json.dumps({"slices": {"genre": {"counts": {"Pop": 10}}}})

        def blob_side_effect(path):
            blob = MagicMock()
            blob.exists.return_value = True
            if "bias" in path:
                blob.download_as_text.return_value = bias_json
            else:
                blob.download_as_text.return_value = "header\nrow1\n"
            return blob

        mock_bucket.blob.side_effect = blob_side_effect

        from backend.pipelines.api.alert_manager import compute_session_anomalies
        result = compute_session_anomalies(
            bucket_name="test-bucket",
            session_id="abc123",
            valid_path="custom/valid.csv",
            invalid_path="custom/invalid.csv",
            bias_metrics_path="custom/bias.json"
        )

        blob_calls = [call[0][0] for call in mock_bucket.blob.call_args_list]
        assert "custom/valid.csv" in blob_calls
        assert "custom/invalid.csv" in blob_calls
        assert "custom/bias.json" in blob_calls
