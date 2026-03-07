import base64
import json
import pytest
from unittest.mock import patch, MagicMock


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


def _encode(session_id: str) -> str:
    return base64.b64encode(session_id.encode()).decode()


class TestHealthEndpoint:

    def test_health_returns_ok(self, client):
        res = client.get("/")
        assert res.status_code == 200
        assert res.data == b"OK"


class TestPubsubWorker:

    def test_valid_message_accepted(self, client):
        payload = {"message": {"data": _encode("sess_abc123")}}
        with patch("backend.pipelines.api.server.threading.Thread") as mock_thread:
            mock_thread.return_value.start = MagicMock()
            res = client.post("/", json=payload)
        assert res.status_code == 202
        assert json.loads(res.data)["session_id"] == "sess_abc123"

    def test_missing_body_returns_400(self, client):
        res = client.post("/", data="not json", content_type="text/plain")
        assert res.status_code == 400

    def test_missing_data_field_returns_400(self, client):
        assert client.post("/", json={"message": {}}).status_code == 400

    def test_invalid_base64_returns_400(self, client):
        assert client.post("/", json={"message": {"data": "not_valid_base64!!!"}}).status_code == 400

    def test_thread_is_started(self, client):
        payload = {"message": {"data": _encode("sess_xyz")}}
        with patch("backend.pipelines.api.server.threading.Thread") as mock_thread_cls:
            mock_thread_instance = MagicMock()
            mock_thread_cls.return_value = mock_thread_instance
            client.post("/", json=payload)
        mock_thread_instance.start.assert_called_once()
