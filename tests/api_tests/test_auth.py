from backend.pipelines.api.auth import GoogleAuthClient
from unittest.mock import patch

def test_get_access_token():
    with patch("requests.post") as mock_post:
        mock_post.return_value.json.return_value = {"access_token":"token123"}
        mock_post.return_value.raise_for_status = lambda: None

        auth = GoogleAuthClient("uri","cid","secret","redir","refresh")
        token = auth.get_access_token()
        assert token == "token123"
