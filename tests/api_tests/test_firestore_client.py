from backend.pipelines.api.firestore_client import FirestoreClient
from unittest.mock import MagicMock

def test_get_session_users(monkeypatch):
    fake_doc = MagicMock()
    fake_doc.exists = True
    fake_doc.to_dict.return_value = {"users":[{"user_id":"u1","refresh_token":"r1"}]}
    mock_client = MagicMock()
    mock_client.collection.return_value.document.return_value.get.return_value = fake_doc
    monkeypatch.setattr("backend.pipelines.api.firestore_client.firestore.Client", lambda **kwargs: mock_client)

    fs = FirestoreClient("proj","db")
    users = fs.get_session_users("sess")
    assert users == [("u1","r1")]
