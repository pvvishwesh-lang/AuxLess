import json
import os
import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from unittest.mock import patch
from backend.pipelines.stream_pipeline import ParseEventFn, _require_env

class TestRequireEnv:

    def test_missing_env_var_raises_clearly(self):
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(EnvironmentError, match="GCP_PROJECT_ID"):
                _require_env("GCP_PROJECT_ID")

    def test_present_env_var_returns_value(self):
        with patch.dict(os.environ, {"GCP_PROJECT_ID": "my-project"}):
            assert _require_env("GCP_PROJECT_ID") == "my-project"


class TestParseEventFnPipeline:

    def test_pipeline_parses_valid_and_rejects_invalid(self):
        raw = [
            json.dumps({"room_id": "r1", "song_id": "s1", "event_type": "like",    "user_id": "u1"}).encode(),
            json.dumps({"room_id": "r1", "song_id": "s2", "event_type": "dislike", "user_id": "u2"}).encode(),
            b"invalid json !!",
            json.dumps({"song_id": "s3", "event_type": "like"}).encode(),  # missing room_id
        ]
        with TestPipeline() as p:
            results = (
                p
                | beam.Create(raw)
                | beam.ParDo(ParseEventFn())
            )
            assert_that(results, lambda items: len(items) == 2)
