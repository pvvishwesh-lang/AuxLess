import sys
from unittest.mock import patch, MagicMock
sys.modules["functions_framework"] = MagicMock()
sys.modules["google.events"] = MagicMock()
sys.modules["google.events.cloud"] = MagicMock()
sys.modules["google.events.cloud.firestore_v1"] = MagicMock()

import base64
import json
import os
import pytest


def _make_cloud_event(session_id: str):
    encoded = base64.b64encode(json.dumps({"session_id": session_id}).encode()).decode()
    mock_event = MagicMock()
    mock_event.data = {"message": {"data": encoded}}
    return mock_event


ENV = {
    "PROJECT_ID":              "test-project",
    "REGION":                  "us-central1",
    "BUCKET":                  "test-bucket",
    "FIRESTORE_DATABASE":      "test-db",
    "STREAMING_TEMPLATE_PATH": "gs://test-bucket/templates/feedback_streaming",
}


class TestTriggerStreamingPipeline:

    def _run(self, session_id: str, dataflow_response: dict = None):
        mock_job           = {"id": "job_abc123"}
        dataflow_response  = dataflow_response or {"job": mock_job}

        mock_execute   = MagicMock(return_value=dataflow_response)
        mock_launch    = MagicMock(return_value=MagicMock(execute=mock_execute))
        mock_flex      = MagicMock(return_value=MagicMock(launch=mock_launch))
        mock_locations = MagicMock(return_value=MagicMock(flexTemplates=mock_flex))
        mock_projects  = MagicMock(return_value=MagicMock(locations=mock_locations))
        mock_dataflow  = MagicMock(return_value=MagicMock(projects=mock_projects))

        cloud_event = _make_cloud_event(session_id)
        with patch.dict(os.environ, ENV):
            from backend.functions.google_cloud_function import trigger_streaming_pipeline
            with patch("backend.functions.google_cloud_function.build", mock_dataflow):
                trigger_streaming_pipeline(cloud_event)

        return mock_launch, mock_execute

    def test_launches_dataflow_job(self):
        _, mock_execute = self._run("sess_abc123")
        mock_execute.assert_called_once()

    def test_job_name_contains_session_id(self):
        mock_launch, _ = self._run("sess_abc123")
        launch_body = mock_launch.call_args[1]["body"]
        assert "sess_abc123"[:10] in launch_body["launchParameter"]["jobName"]

    def test_correct_subscription_passed(self):
        mock_launch, _ = self._run("sess_xyz999")
        params = mock_launch.call_args[1]["body"]["launchParameter"]["parameters"]
        assert "feedback-events-sub" in params["input_subscription"]
        assert "test-project"        in params["input_subscription"]

    def test_correct_project_passed(self):
        mock_launch, _ = self._run("sess_xyz999")
        params = mock_launch.call_args[1]["body"]["launchParameter"]["parameters"]
        assert params["firestore_project"] == "test-project"
        assert params["bucket"]            == "test-bucket"

    def test_template_path_passed(self):
        mock_launch, _ = self._run("sess_xyz999")
        container_path = mock_launch.call_args[1]["body"]["launchParameter"]["containerSpecGcsPath"]
        assert container_path == "gs://test-bucket/templates/feedback_streaming"

    def test_invalid_base64_does_not_raise(self):
        mock_event = MagicMock()
        mock_event.data = {"message": {"data": "not_valid_base64!!!"}}

        with patch.dict(os.environ, ENV):
            from backend.functions.google_cloud_function import trigger_streaming_pipeline
            with patch("backend.functions.google_cloud_function.build") as mock_build:
                trigger_streaming_pipeline(mock_event)

        mock_build.assert_not_called()

    def test_missing_session_id_in_payload_does_not_raise(self):
        encoded = base64.b64encode(json.dumps({"other_key": "value"}).encode()).decode()
        mock_event = MagicMock()
        mock_event.data = {"message": {"data": encoded}}

        with patch.dict(os.environ, ENV):
            from backend.functions.google_cloud_function import trigger_streaming_pipeline
            with patch("backend.functions.google_cloud_function.build") as mock_build:
                trigger_streaming_pipeline(mock_event)

        mock_build.assert_not_called()

    def test_dataflow_api_failure_is_logged(self):
        mock_execute   = MagicMock(side_effect=Exception("Dataflow API error"))
        mock_launch    = MagicMock(return_value=MagicMock(execute=mock_execute))
        mock_flex      = MagicMock(return_value=MagicMock(launch=mock_launch))
        mock_locations = MagicMock(return_value=MagicMock(flexTemplates=mock_flex))
        mock_projects  = MagicMock(return_value=MagicMock(locations=mock_locations))
        mock_dataflow  = MagicMock(return_value=MagicMock(projects=mock_projects))

        cloud_event = _make_cloud_event("sess_fail")

        with patch.dict(os.environ, ENV):
            from backend.functions.google_cloud_function import trigger_streaming_pipeline
            with patch("backend.functions.google_cloud_function.build", mock_dataflow):
                try:
                    trigger_streaming_pipeline(cloud_event)
                except Exception:
                    pytest.fail("Cloud Function propagated an exception")
