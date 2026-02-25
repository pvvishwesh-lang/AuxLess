import requests
import logging
from backend.pipelines.api.retry_utils import retry

logger = logging.getLogger(__name__)

class YoutubeClient:
    BASE_URL = "https://www.googleapis.com/youtube/v3"
    def __init__(self, access_token: str):
        self.headers = {"Authorization": f"Bearer {access_token}"}
    @retry(retries=3, delay=2)
    def get_playlists(self) -> list:
        url = f"{self.BASE_URL}/playlists"
        params = {"part": "snippet,contentDetails", "mine": "true", "maxResults": 50}
        all_playlists = []
        while True:
            res = requests.get(url, headers=self.headers, params=params, timeout=10)
            res.raise_for_status()
            data = res.json()
            all_playlists.extend(data.get("items", []))
            if "nextPageToken" not in data:
                break
            params["pageToken"] = data["nextPageToken"]
        return all_playlists

    @retry(retries=3, delay=2)
    def get_playlist_tracks(self, playlist_id: str) -> list:
        url = f"{self.BASE_URL}/playlistItems"
        params = {
            "part": "snippet,contentDetails",
            "playlistId": playlist_id,
            "maxResults": 50
        }
        all_tracks = []
        while True:
            res = requests.get(url, headers=self.headers, params=params, timeout=10)
            res.raise_for_status()
            data = res.json()
            all_tracks.extend(data.get("items", []))
            if "nextPageToken" not in data:
                break
            params["pageToken"] = data["nextPageToken"]
        return all_tracks

    @retry(retries=3, delay=2)
    def get_video_stats(self, video_ids: list) -> dict:
        url = f"{self.BASE_URL}/videos"
        params = {"part": "statistics", "id": ",".join(video_ids)}
        res = requests.get(url, headers=self.headers, params=params, timeout=10)
        res.raise_for_status()
        data = res.json()
        stats_map = {}
        for item in data.get("items", []):
            vid = item["id"]
            stats = item.get("statistics", {})
            stats_map[vid] = (
                int(stats.get("viewCount", 0)),
                int(stats.get("likeCount", 0)),
                int(stats.get("commentCount", 0))
            )
        return stats_map
