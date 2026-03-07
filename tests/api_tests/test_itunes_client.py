import pytest
import requests
from unittest.mock import patch, MagicMock
from backend.pipelines.api.itunes_client import ItunesClient


@pytest.fixture
def client():
    return ItunesClient()


@pytest.fixture
def mock_itunes_response():
    return {
        "results": [{
            "primaryGenreName": "Pop",
            "artistName":       "The Weeknd",
            "country":          "USA",
            "collectionName":   "After Hours",
            "collectionId":     111,
            "trackTimeMillis":  200000
        }]
    }


class TestItunesClient:

    def test_returns_correct_genre(self, client, mock_itunes_response):
        mock_res = MagicMock()
        mock_res.raise_for_status.return_value = None
        mock_res.json.return_value = mock_itunes_response

        with patch("requests.get", return_value=mock_res):
            genre, artist, *_ = client.get_genre("Blinding Lights")

        assert genre  == "Pop"
        assert artist == "The Weeknd"

    def test_no_results_returns_unknown(self, client):
        mock_res = MagicMock()
        mock_res.raise_for_status.return_value = None
        mock_res.json.return_value = {"results": []}

        with patch("requests.get", return_value=mock_res):
            genre, *_ = client.get_genre("Nonexistent Song 12345xyz")

        assert genre == "Unknown"

    def test_http_error_is_raised(self, client):
        mock_res = MagicMock()
        mock_res.raise_for_status.side_effect = requests.HTTPError("429 Too Many Requests")

        with patch("requests.get", return_value=mock_res):
            with pytest.raises(requests.HTTPError):
                client.get_genre("Any Song")

    def test_unexpected_exception_returns_unknown(self, client):
        with patch("requests.get", side_effect=ConnectionError("network down")):
            genre, *_ = client.get_genre("Any Song")
        assert genre == "Unknown"

    def test_query_combines_title_and_artist(self, client, mock_itunes_response):
        mock_res = MagicMock()
        mock_res.raise_for_status.return_value = None
        mock_res.json.return_value = mock_itunes_response

        with patch("requests.get", return_value=mock_res) as mock_get:
            client.get_genre("Blinding Lights", artist_name="The Weeknd")

        params = mock_get.call_args[1]["params"]
        assert "Blinding Lights" in params["term"]
        assert "The Weeknd"      in params["term"]
