import pytest
import requests
from unittest.mock import patch, MagicMock
from backend.pipelines.api.auth import GoogleAuthClient


@pytest.fixture
def auth_client():
    return GoogleAuthClient(
        token_uri="https://oauth2.googleapis.com/token",
        client_id="client_id",
        client_secret="client_secret",
        redirect_uri="http://localhost",
        refresh_token="refresh_token"
    )


class TestGoogleAuthClient:

    def test_returns_access_token(self, auth_client):
        mock_res = MagicMock()
        mock_res.raise_for_status.return_value = None
        mock_res.json.return_value = {"access_token": "ya29.test_token"}

        with patch("requests.post", return_value=mock_res):
            assert auth_client.get_access_token() == "ya29.test_token"

    def test_raises_on_http_error(self, auth_client):
        mock_res = MagicMock()
        mock_res.raise_for_status.side_effect = requests.HTTPError("401 Unauthorized")

        with patch("requests.post", return_value=mock_res):
            with pytest.raises(requests.HTTPError):
                auth_client.get_access_token()

    def test_correct_payload_sent(self, auth_client):
        mock_res = MagicMock()
        mock_res.raise_for_status.return_value = None
        mock_res.json.return_value = {"access_token": "token"}

        with patch("requests.post", return_value=mock_res) as mock_post:
            auth_client.get_access_token()

        payload = mock_post.call_args[1]["data"]
        assert payload["grant_type"]    == "refresh_token"
        assert payload["refresh_token"] == "refresh_token"
        assert payload["client_id"]     == "client_id"
