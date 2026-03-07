import pytest
from backend.pipelines.api.validation_utils import derive_fields, validate_record

COLUMNS = [
    "playlist_name", "track_title", "artist_name", "video_id", "genre",
    "country", "collection_name", "collection_id", "trackTimeMillis",
    "trackTimeSeconds", "view_count", "like_count", "comment_count",
    "like_to_view_ratio", "comment_to_view_ratio"
]


class TestDeriveFields:

    def test_normal_record(self, valid_raw_record):
        result = derive_fields(dict(valid_raw_record))
        assert result["trackTimeMillis"]  == 200000
        assert result["trackTimeSeconds"] == 200
        assert result["view_count"]       == 1000000
        assert result["like_count"]       == 50000
        assert result["comment_count"]    == 3000

    def test_like_to_view_ratio(self, valid_raw_record):
        result = derive_fields(dict(valid_raw_record))
        assert result["like_to_view_ratio"]    == pytest.approx(0.05,  abs=1e-4)
        assert result["comment_to_view_ratio"] == pytest.approx(0.003, abs=1e-4)

    def test_zero_view_count_no_division_error(self, valid_raw_record):
        valid_raw_record["view_count"] = "0"
        result = derive_fields(dict(valid_raw_record))
        assert result["like_to_view_ratio"]    == 0.0
        assert result["comment_to_view_ratio"] == 0.0

    def test_none_track_time_millis(self, valid_raw_record):
        valid_raw_record["trackTimeMillis"] = None
        result = derive_fields(dict(valid_raw_record))
        assert result["trackTimeMillis"]  == 0
        assert result["trackTimeSeconds"] == 0

    def test_empty_string_track_time_millis(self, valid_raw_record):
        valid_raw_record["trackTimeMillis"] = ""
        result = derive_fields(dict(valid_raw_record))
        assert result["trackTimeMillis"] == 0

    def test_invalid_string_track_time_millis(self, valid_raw_record):
        valid_raw_record["trackTimeMillis"] = "not_a_number"
        result = derive_fields(dict(valid_raw_record))
        assert result["trackTimeMillis"] == 0

    def test_float_string_track_time_millis(self, valid_raw_record):
        valid_raw_record["trackTimeMillis"] = "199999.9"
        result = derive_fields(dict(valid_raw_record))
        assert result["trackTimeMillis"] == 199999

    def test_negative_counts_coerced(self, valid_raw_record):
        valid_raw_record["like_count"] = "-5"
        result = derive_fields(dict(valid_raw_record))
        assert result["like_count"] == -5

    def test_missing_counts_default_zero(self, valid_raw_record):
        del valid_raw_record["like_count"]
        result = derive_fields(dict(valid_raw_record))
        assert result["like_count"] == 0


class TestValidateRecord:

    def test_valid_record_passes(self, derived_record):
        validate_record(derived_record, COLUMNS)  

    def test_missing_column_raises(self, derived_record):
        del derived_record["video_id"]
        with pytest.raises(ValueError, match="Missing required columns"):
            validate_record(derived_record, COLUMNS)

    def test_multiple_missing_columns(self, derived_record):
        del derived_record["video_id"]
        del derived_record["genre"]
        with pytest.raises(ValueError) as exc_info:
            validate_record(derived_record, COLUMNS)
        assert "video_id" in str(exc_info.value)
        assert "genre"    in str(exc_info.value)

    def test_empty_record_raises(self):
        with pytest.raises(ValueError):
            validate_record({}, COLUMNS)
