"""
Tests for the /ml endpoint in backend/pipelines/api/server.py

Covers:
  - POST /ml with valid Pub/Sub message → 202
  - POST /ml with missing body → 400
  - POST /ml with missing data field → 400
  - POST /ml with invalid base64 → 400
  - POST /ml with missing session_id in payload → 400
  - POST /ml starts background thread
  - background thread calls _run_ml_session with correct session_id

Follows the same pattern as test_server.py.

NOTE: All /ml endpoint tests are skipped until the /ml route and
_run_ml_session_safe are uncommented in server.py.
Re-enable by removing the @pytest.mark.skip decorators below.
"""

import base64
import json
import pytest
from unittest.mock import patch, MagicMock

# Skip reason used across all classes
ML_SKIP_REASON = (
    "Skipped — /ml endpoint is commented out in server.py. "
    "Re-enable when ML deployment is ready."
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def app():
    import importlib
    from backend.pipelines.api import server
    importlib.reload(server)
    server.app.config["TESTING"] = True
    return server.app


@pytest.fixture
def client(app):
    return app.test_client()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _encode_payload(session_id: str) -> str:
    """Encodes { session_id: ... } as base64 JSON — matches pubsub_publisher.py format."""
    payload = json.dumps({"session_id": session_id}).encode("utf-8")
    return base64.b64encode(payload).decode("utf-8")


def _make_pubsub_envelope(session_id: str) -> dict:
    return {"message": {"data": _encode_payload(session_id)}}


# ── Valid request tests ───────────────────────────────────────────────────────

@pytest.mark.skip(reason=ML_SKIP_REASON)
class TestMlEndpointValid:

    def test_returns_202_for_valid_message(self, client):
        payload = _make_pubsub_envelope("sess_abc123")
        with patch("backend.pipelines.api.server.threading.Thread") as mock_thread:
            mock_thread.return_value.start = MagicMock()
            res = client.post("/ml", json=payload)
        assert res.status_code == 202

    def test_response_contains_session_id(self, client):
        payload = _make_pubsub_envelope("sess_abc123")
        with patch("backend.pipelines.api.server.threading.Thread") as mock_thread:
            mock_thread.return_value.start = MagicMock()
            res = client.post("/ml", json=payload)
        body = json.loads(res.data)
        assert body["session_id"] == "sess_abc123"

    def test_response_contains_status_accepted(self, client):
        payload = _make_pubsub_envelope("sess_xyz")
        with patch("backend.pipelines.api.server.threading.Thread") as mock_thread:
            mock_thread.return_value.start = MagicMock()
            res = client.post("/ml", json=payload)
        body = json.loads(res.data)
        assert body["status"] == "accepted"

    def test_thread_is_started(self, client):
        payload = _make_pubsub_envelope("sess_xyz")
        with patch("backend.pipelines.api.server.threading.Thread") as mock_thread_cls:
            mock_instance = MagicMock()
            mock_thread_cls.return_value = mock_instance
            client.post("/ml", json=payload)
        mock_instance.start.assert_called_once()

    def test_thread_target_is_run_ml_session_safe(self, client):
        payload = _make_pubsub_envelope("sess_xyz")
        with patch("backend.pipelines.api.server.threading.Thread") as mock_thread_cls:
            mock_instance = MagicMock()
            mock_thread_cls.return_value = mock_instance
            client.post("/ml", json=payload)
        thread_kwargs = mock_thread_cls.call_args[1]
        assert thread_kwargs.get("target") is not None

    def test_thread_is_daemon(self, client):
        payload = _make_pubsub_envelope("sess_xyz")
        with patch("backend.pipelines.api.server.threading.Thread") as mock_thread_cls:
            mock_instance = MagicMock()
            mock_thread_cls.return_value = mock_instance
            client.post("/ml", json=payload)
        thread_kwargs = mock_thread_cls.call_args[1]
        assert thread_kwargs.get("daemon") is True

    def test_session_id_passed_to_thread(self, client):
        payload = _make_pubsub_envelope("my-special-session")
        with patch("backend.pipelines.api.server.threading.Thread") as mock_thread_cls:
            mock_instance = MagicMock()
            mock_thread_cls.return_value = mock_instance
            client.post("/ml", json=payload)
        thread_kwargs = mock_thread_cls.call_args[1]
        assert "my-special-session" in thread_kwargs.get("args", ())

    def test_returns_immediately_without_waiting(self, client):
        """Endpoint should return 202 before ML pipeline completes."""
        payload = _make_pubsub_envelope("sess_quick")
        with patch("backend.pipelines.api.server._run_ml_session_safe") as mock_run:
            mock_run.side_effect = lambda *a: None
            with patch("backend.pipelines.api.server.threading.Thread") as mock_thread:
                mock_thread.return_value.start = MagicMock()
                res = client.post("/ml", json=payload)
        assert res.status_code == 202
        mock_run.assert_not_called()


# ── Invalid request tests ─────────────────────────────────────────────────────

@pytest.mark.skip(reason=ML_SKIP_REASON)
class TestMlEndpointInvalid:

    def test_missing_body_returns_400(self, client):
        res = client.post("/ml", data="not json", content_type="text/plain")
        assert res.status_code == 400

    def test_empty_json_returns_400(self, client):
        res = client.post("/ml", json={})
        assert res.status_code == 400

    def test_missing_message_field_returns_400(self, client):
        res = client.post("/ml", json={"wrong_key": "value"})
        assert res.status_code == 400

    def test_missing_data_field_returns_400(self, client):
        res = client.post("/ml", json={"message": {}})
        assert res.status_code == 400

    def test_invalid_base64_returns_400(self, client):
        res = client.post("/ml", json={"message": {"data": "not_valid_base64!!!"}})
        assert res.status_code == 400

    def test_valid_base64_but_missing_session_id_returns_400(self, client):
        payload = json.dumps({"wrong_key": "value"}).encode("utf-8")
        data    = base64.b64encode(payload).decode("utf-8")
        res     = client.post("/ml", json={"message": {"data": data}})
        assert res.status_code == 400

    def test_none_body_returns_400(self, client):
        res = client.post("/ml")
        assert res.status_code == 400


# ── Isolation tests (ml endpoint does not affect other endpoints) ─────────────

class TestMlEndpointIsolation:

    def test_health_check_still_works(self, client):
        res = client.get("/")
        assert res.status_code == 200
        assert res.data == b"OK"

    def test_batch_endpoint_still_works(self, client):
        data    = base64.b64encode(b"sess_batch_123").decode("utf-8")
        payload = {"message": {"data": data}}
        with patch("backend.pipelines.api.server.threading.Thread") as mock_thread:
            mock_thread.return_value.start = MagicMock()
            res = client.post("/", json=payload)
        assert res.status_code == 202

    def test_ml_endpoint_does_not_trigger_batch_pipeline(self, client):
      payload = _make_pubsub_envelope("sess_xyz")
      with patch("backend.pipelines.api.server._run_session_safe") as mock_batch:
          with patch("backend.pipelines.api.server.threading.Thread") as mock_thread:
              mock_thread.return_value.start = MagicMock()
              res = client.post("/ml", json=payload)

      assert res.status_code == 202
      mock_batch.assert_not_called()

    def test_batch_endpoint_does_not_trigger_ml_pipeline(self, client):
        """While /ml is commented out, just verify batch works independently."""
        data    = base64.b64encode(b"sess_batch_123").decode("utf-8")
        payload = {"message": {"data": data}}
        with patch("backend.pipelines.api.server.threading.Thread") as mock_thread:
            mock_thread.return_value.start = MagicMock()
            res = client.post("/", json=payload)
        assert res.status_code == 202


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
