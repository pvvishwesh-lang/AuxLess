import pytest
import requests
from unittest.mock import patch, MagicMock
from backend.pipelines.api.youtube_client import YoutubeClient


@pytest.fixture
def client():
    return YoutubeClient(access_token="test_token")


class TestYoutubeClient:

    def test_get_playlists_returns_items(self, client):
        mock_res = MagicMock()
        mock_res.raise_for_status.return_value = None
        mock_res.json.return_value = {"items": [{"id": "PL1", "snippet": {"title": "Chill"}}]}

        with patch("requests.get", return_value=mock_res):
            playlists = client.get_playlists()

        assert len(playlists) == 1
        assert playlists[0]["id"] == "PL1"

    def test_get_playlists_handles_pagination(self, client):
        page1 = MagicMock()
        page1.raise_for_status.return_value = None
        page1.json.return_value = {"items": [{"id": "PL1"}], "nextPageToken": "tok"}

        page2 = MagicMock()
        page2.raise_for_status.return_value = None
        page2.json.return_value = {"items": [{"id": "PL2"}]}

        with patch("requests.get", side_effect=[page1, page2]):
            assert len(client.get_playlists()) == 2

    def test_get_video_stats_returns_correct_map(self, client):
        mock_res = MagicMock()
        mock_res.raise_for_status.return_value = None
        mock_res.json.return_value = {
            "items": [{
                "id": "abc123",
                "statistics": {"viewCount": "1000", "likeCount": "50", "commentCount": "10"}
            }]
        }
        with patch("requests.get", return_value=mock_res):
            assert client.get_video_stats(["abc123"])["abc123"] == (1000, 50, 10)

    def test_get_video_stats_missing_stats_defaults_to_zero(self, client):
        mock_res = MagicMock()
        mock_res.raise_for_status.return_value = None
        mock_res.json.return_value = {"items": [{"id": "abc123", "statistics": {}}]}

        with patch("requests.get", return_value=mock_res):
            assert client.get_video_stats(["abc123"])["abc123"] == (0, 0, 0)

    def test_http_error_is_raised_for_retry(self, client):
        mock_res = MagicMock()
        mock_res.raise_for_status.side_effect = requests.HTTPError("403")

        with patch("requests.get", return_value=mock_res), patch("time.sleep"):
            with pytest.raises(requests.HTTPError):
                client.get_playlists()
