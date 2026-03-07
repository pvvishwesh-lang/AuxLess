import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from backend.pipelines.api.schema_validator import (
    _generate_statistics,
    _validate_schema,
    run_schema_validation,
)


@pytest.fixture
def valid_df():
    return pd.DataFrame([{
        "playlist_name":         "Chill",
        "track_title":           "Blinding Lights",
        "artist_name":           "The Weeknd",
        "video_id":              "abc123",
        "genre":                 "Pop",
        "country":               "USA",
        "collection_name":       "After Hours",
        "collection_id":         "111",
        "trackTimeMillis":       200000,
        "trackTimeSeconds":      200,
        "view_count":            1000000,
        "like_count":            50000,
        "comment_count":         3000,
        "like_to_view_ratio":    0.05,
        "comment_to_view_ratio": 0.003,
    }])


class TestGenerateStatistics:

    def test_returns_stats_for_all_columns(self, valid_df):
        stats = _generate_statistics(valid_df)
        for col in valid_df.columns:
            assert col in stats

    def test_numeric_columns_have_mean(self, valid_df):
        stats = _generate_statistics(valid_df)
        assert "mean" in stats["view_count"]
        assert "std"  in stats["view_count"]

    def test_string_columns_have_no_mean(self, valid_df):
        assert "mean" not in _generate_statistics(valid_df)["track_title"]

    def test_missing_count_is_zero_for_clean_data(self, valid_df):
        assert _generate_statistics(valid_df)["video_id"]["missing_count"] == 0


class TestValidateSchema:

    def test_valid_df_has_no_violations(self, valid_df):
        assert _validate_schema(valid_df) == []

    def test_missing_column_flagged(self, valid_df):
        df = valid_df.drop(columns=["video_id"])
        violations = _validate_schema(df)
        assert any(v["column"] == "video_id" and v["issue"] == "missing_column" for v in violations)

    def test_null_in_non_nullable_column_flagged(self, valid_df):
        valid_df.loc[0, "video_id"] = None
        violations = _validate_schema(valid_df)
        assert any(v["column"] == "video_id" and v["issue"] == "unexpected_nulls" for v in violations)

    def test_value_below_min_flagged(self, valid_df):
        valid_df.loc[0, "view_count"] = -1
        violations = _validate_schema(valid_df)
        assert any(v["column"] == "view_count" and "below_min" in v["issue"] for v in violations)

    def test_ratio_above_max_flagged(self, valid_df):
        valid_df.loc[0, "like_to_view_ratio"] = 1.5
        violations = _validate_schema(valid_df)
        assert any(v["column"] == "like_to_view_ratio" and "above_max" in v["issue"] for v in violations)


class TestRunSchemaValidation:

    def test_valid_data_returns_schema_valid_true(self, valid_df):
        mock_blob   = MagicMock()
        mock_blob.download_as_text.return_value = valid_df.to_csv(index=False)
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        with patch("backend.pipelines.api.schema_validator.storage.Client", return_value=mock_client):
            result = run_schema_validation("my-bucket", "sess_001")

        assert result["schema_valid"] is True
        assert result["row_count"]    == 1

    def test_invalid_data_returns_schema_valid_false(self, valid_df):
        valid_df.loc[0, "view_count"] = -100
        mock_blob   = MagicMock()
        mock_blob.download_as_text.return_value = valid_df.to_csv(index=False)
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        with patch("backend.pipelines.api.schema_validator.storage.Client", return_value=mock_client):
            result = run_schema_validation("my-bucket", "sess_001")

        assert result["schema_valid"] is False
        assert len(result["schema_violations"]) > 0
