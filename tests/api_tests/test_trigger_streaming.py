import base64
import json
import os
import pytest
from unittest.mock import patch, MagicMock


def _make_cloud_event(session_id: str):
    """Builds a mock CloudEvent that mirrors what Pub/Sub push delivers."""
    encoded = base64.b64encode(json.dumps({"session_id": session_id}).encode()).decode()
    mock_event = MagicMock()
    mock_event.data = {"message": {"data": encoded}}
    return mock_event


ENV = {
    "PROJECT_ID":             "test-project",
    "REGION":                 "us-central1",
    "BUCKET":                 "test-bucket",
    "FIRESTORE_DATABASE":     "test-db",
    "STREAMING_TEMPLATE_PATH": "gs://test-bucket/templates/feedback_streaming",
}


class TestTriggerStreamingPipeline:

    def _run(self, session_id: str, dataflow_response: dict = None):
        """Helper — runs the cloud function with a mocked Dataflow API call."""
        mock_job = {"id": "job_abc123", "name": f"feedback-streaming-{session_id[:10]}"}
        dataflow_response = dataflow_response or {"job": mock_job}

        # Mock the full Dataflow API call chain:
        # build("dataflow") → .projects().locations().flexTemplates().launch().execute()
        mock_execute   = MagicMock(return_value=dataflow_response)
        mock_launch    = MagicMock(return_value=MagicMock(execute=mock_execute))
        mock_flex      = MagicMock(return_value=MagicMock(launch=mock_launch))
        mock_locations = MagicMock(return_value=MagicMock(flexTemplates=mock_flex))
        mock_projects  = MagicMock(return_value=MagicMock(locations=mock_locations))
        mock_dataflow  = MagicMock(return_value=MagicMock(projects=mock_projects))

        cloud_event = _make_cloud_event(session_id)

        with patch("cloud_functions.trigger_streaming.main.build", mock_dataflow):
            with patch.dict(os.environ, ENV):
                from cloud_functions.trigger_streaming.main import trigger_streaming_pipeline
                trigger_streaming_pipeline(cloud_event)

        return mock_launch, mock_execute

    def test_launches_dataflow_job(self):
        mock_launch, mock_execute = self._run("sess_abc123")
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
        """Malformed Pub/Sub message should log and return, not crash."""
        mock_event = MagicMock()
        mock_event.data = {"message": {"data": "not_valid_base64!!!"}}

        with patch("cloud_functions.trigger_streaming.main.build") as mock_build:
            with patch.dict(os.environ, ENV):
                from cloud_functions.trigger_streaming.main import trigger_streaming_pipeline
                trigger_streaming_pipeline(mock_event)  # should not raise

        mock_build.assert_not_called()

    def test_missing_session_id_in_payload_does_not_raise(self):
        """Pub/Sub message with valid base64 but missing session_id key."""
        encoded = base64.b64encode(json.dumps({"other_key": "value"}).encode()).decode()
        mock_event = MagicMock()
        mock_event.data = {"message": {"data": encoded}}

        with patch("cloud_functions.trigger_streaming.main.build") as mock_build:
            with patch.dict(os.environ, ENV):
                from cloud_functions.trigger_streaming.main import trigger_streaming_pipeline
                trigger_streaming_pipeline(mock_event)

        mock_build.assert_not_called()

    def test_dataflow_api_failure_is_logged(self):
        """If Dataflow raises, the function should log the error, not crash."""
        mock_execute   = MagicMock(side_effect=Exception("Dataflow API error"))
        mock_launch    = MagicMock(return_value=MagicMock(execute=mock_execute))
        mock_flex      = MagicMock(return_value=MagicMock(launch=mock_launch))
        mock_locations = MagicMock(return_value=MagicMock(flexTemplates=mock_flex))
        mock_projects  = MagicMock(return_value=MagicMock(projects=mock_locations))
        mock_dataflow  = MagicMock(return_value=MagicMock(projects=mock_projects))

        cloud_event = _make_cloud_event("sess_fail")

        with patch("cloud_functions.trigger_streaming.main.build", mock_dataflow):
            with patch.dict(os.environ, ENV):
                from cloud_functions.trigger_streaming.main import trigger_streaming_pipeline
                # Should not propagate the exception — Cloud Functions must return cleanly
                # or Pub/Sub will retry indefinitely
                try:
                    trigger_streaming_pipeline(cloud_event)
                except Exception:
                    pytest.fail("Cloud Function propagated an exception — Pub/Sub will retry forever")
