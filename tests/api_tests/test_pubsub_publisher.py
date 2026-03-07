import json
import os
import pytest
from unittest.mock import patch, MagicMock


class TestPublishSessionReady:

    def test_publishes_with_correct_session_id(self):
        mock_future = MagicMock()
        mock_future.result.return_value = "msg_id_123"
        mock_publisher = MagicMock()
        mock_publisher.topic_path.return_value = "projects/proj/topics/session-ready"
        mock_publisher.publish.return_value = mock_future

        with patch("backend.pipelines.api.pubsub_publisher.pubsub_v1.PublisherClient",
                   return_value=mock_publisher):
            with patch.dict(os.environ, {"PROJECT_ID": "my-project", "SESSION_READY_TOPIC": "session-ready"}):
                from backend.pipelines.api.pubsub_publisher import publish_session_ready
                publish_session_ready("sess_abc123")

        payload = json.loads(mock_publisher.publish.call_args[1]["data"].decode())
        assert payload["session_id"] == "sess_abc123"


class TestPublishFeedbackEvent:

    def test_publishes_feedback_event(self):
        mock_future = MagicMock()
        mock_future.result.return_value = "msg_id_456"
        mock_publisher = MagicMock()
        mock_publisher.topic_path.return_value = "projects/proj/topics/feedback-events"
        mock_publisher.publish.return_value = mock_future

        event = {"room_id": "r1", "song_id": "s1", "action": "like", "user_id": "u1"}

        with patch("backend.pipelines.api.pubsub_publisher.pubsub_v1.PublisherClient",
                   return_value=mock_publisher):
            with patch.dict(os.environ, {"PROJECT_ID": "my-project", "FEEDBACK_TOPIC": "feedback-events"}):
                from backend.pipelines.api.pubsub_publisher import publish_feedback_event
                publish_feedback_event(event)

        mock_publisher.publish.assert_called_once()
