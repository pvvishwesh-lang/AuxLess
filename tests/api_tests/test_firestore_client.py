import pytest
from unittest.mock import patch, MagicMock
from backend.pipelines.api.firestore_client import FirestoreClient


def _make_fs_client(session_data=None, exists=True):
    mock_doc = MagicMock()
    mock_doc.exists = exists
    mock_doc.to_dict.return_value = session_data or {}

    mock_docref = MagicMock()
    mock_docref.get.return_value = mock_doc
    mock_docref.update = MagicMock()

    mock_col = MagicMock()
    mock_col.document.return_value = mock_docref

    mock_db = MagicMock()
    mock_db.collection.return_value = mock_col

    with patch("backend.pipelines.api.firestore_client.firestore.Client", return_value=mock_db):
        client = FirestoreClient("proj", "db")

    return client, mock_docref


class TestFirestoreClient:

    def test_get_session_users_returns_active_users(self):
        data = {
            "status": "running",
            "users": [
                {"user_id": "u1", "refresh_token": "rt1", "isactive": True},
                {"user_id": "u2", "refresh_token": "rt2", "isactive": False},
                {"user_id": "",   "refresh_token": "rt3", "isactive": True},
            ]
        }
        client, _ = _make_fs_client(session_data=data)
        users = client.get_session_users("sess_001")
        assert len(users) == 1
        assert users[0] == ("u1", "rt1")

    def test_get_session_users_raises_if_not_exists(self):
        client, _ = _make_fs_client(exists=False)
        with pytest.raises(RuntimeError, match="does not exist"):
            client.get_session_users("sess_missing")

    def test_get_session_status_returns_status(self):
        client, _ = _make_fs_client(session_data={"status": "done"})
        assert client.get_session_status("sess_001") == "done"

    def test_get_session_status_returns_none_if_not_exists(self):
        client, _ = _make_fs_client(exists=False)
        assert client.get_session_status("sess_missing") is None

    def test_update_session_status_calls_update(self):
        client, mock_docref = _make_fs_client(session_data={"status": "running"})
        client.update_session_status("sess_001", "done")
        mock_docref.update.assert_called_once_with({"status": "done"})
