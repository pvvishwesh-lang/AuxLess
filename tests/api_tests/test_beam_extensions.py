import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from unittest.mock import MagicMock, patch
from backend.pipelines.api.beam_extensions import ValidatingDoFn


@pytest.fixture
def valid_record():
    return {
        "playlist_name": "Chill", "track_title": "Song", "artist_name": "Artist",
        "video_id": "abc123", "genre": "Pop", "country": "USA",
        "collection_name": "Album", "collection_id": "111",
        "trackTimeMillis": 200000, "view_count": 1000,
        "like_count": 50, "comment_count": 10,
    }


class TestValidatingDoFn:

    def test_valid_record_passes_to_main_output(self, valid_record):
        with patch("backend.pipelines.api.beam_extensions.get_logger", return_value=MagicMock()):
            with TestPipeline() as p:
                results = (
                    p
                    | beam.Create([valid_record])
                    | beam.ParDo(ValidatingDoFn("token", "sess_001", "user_001"))
                    .with_outputs("invalid_records", main="valid")
                )
                assert_that(results.valid, lambda x: len(x) == 1)

    def test_invalid_record_routes_to_invalid_output(self):
        with patch("backend.pipelines.api.beam_extensions.get_logger", return_value=MagicMock()):
            with TestPipeline() as p:
                results = (
                    p
                    | beam.Create([{"playlist_name": "X"}])
                    | beam.ParDo(ValidatingDoFn("token", "sess_001", "user_001"))
                    .with_outputs("invalid_records", main="valid")
                )
                assert_that(results.invalid_records, lambda x: len(x) == 1)

    def test_none_element_produces_no_output(self):
        with patch("backend.pipelines.api.beam_extensions.get_logger", return_value=MagicMock()):
            with TestPipeline() as p:
                results = (
                    p
                    | beam.Create([None])
                    | beam.ParDo(ValidatingDoFn("token", "sess_001", "user_001"))
                    .with_outputs("invalid_records", main="valid")
                )
                assert_that(results.valid, equal_to([]))
