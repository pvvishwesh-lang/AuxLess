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


class TestEndSession:
    def test_missing_body_returns_400(self, client):
        res = client.post("/end_session", data="not json", content_type="text/plain")
        assert res.status_code == 400

    def test_missing_session_id_returns_400(self, client):
        res = client.post("/end_session", json={"foo": "bar"})
        assert res.status_code == 400

    @patch("backend.pipelines.api.server.sync_session_to_bigquery")
    @patch("backend.pipelines.api.server._drain_streaming_job")
    @patch("backend.pipelines.api.server.FirestoreClient")
    def test_successful_end_session(self, mock_fs_cls, mock_drain, mock_sync, client):
        mock_fs = MagicMock()
        mock_fs.get_session_users.return_value = [("user1", "token1"), ("user2", "token2")]
        mock_fs_cls.return_value = mock_fs
        mock_drain.return_value = True

        with patch.dict("os.environ", {"PROJECT_ID": "test-proj", "FIRESTORE_DATABASE": "test-db"}):
            res = client.post("/end_session", json={"session_id": "sess_123"})

        assert res.status_code == 200
        data = json.loads(res.data)
        assert data["status"] == "ended"
        assert data["session_id"] == "sess_123"

        mock_drain.assert_called_once_with("test-proj", "sess_123")
        mock_sync.assert_called_once_with(
            session_id="sess_123",
            project_id="test-proj",
            database_id="test-db",
            user_ids=["user1", "user2"]
        )
        mock_fs.update_session_status.assert_called_once_with("sess_123", "ended")

    @patch("backend.pipelines.api.server.sync_session_to_bigquery")
    @patch("backend.pipelines.api.server._drain_streaming_job")
    @patch("backend.pipelines.api.server.FirestoreClient")
    def test_end_session_no_users(self, mock_fs_cls, mock_drain, mock_sync, client):
        mock_fs = MagicMock()
        mock_fs.get_session_users.return_value = []
        mock_fs_cls.return_value = mock_fs

        with patch.dict("os.environ", {"PROJECT_ID": "test-proj", "FIRESTORE_DATABASE": "test-db"}):
            res = client.post("/end_session", json={"session_id": "sess_456"})

        assert res.status_code == 200
        mock_sync.assert_called_once_with(
            session_id="sess_456",
            project_id="test-proj",
            database_id="test-db",
            user_ids=[]
        )

    @patch("backend.pipelines.api.server._drain_streaming_job", side_effect=Exception("drain failed"))
    @patch("backend.pipelines.api.server.FirestoreClient")
    def test_end_session_error_returns_500(self, mock_fs_cls, mock_drain, client):
        mock_fs_cls.return_value = MagicMock()

        with patch.dict("os.environ", {"PROJECT_ID": "test-proj", "FIRESTORE_DATABASE": "test-db"}):
            res = client.post("/end_session", json={"session_id": "sess_789"})

        assert res.status_code == 500
