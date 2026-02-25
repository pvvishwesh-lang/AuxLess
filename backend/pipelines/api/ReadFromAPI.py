import apache_beam as beam
import logging
from backend.pipelines.api.youtube_client import YoutubeClient
from backend.pipelines.api.itunes_client import ItunesClient
from backend.pipelines.api.retry_utils import retry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReadFromAPI(beam.DoFn):
    def __init__(self, access_token: str):
        self.access_token = access_token
    def setup(self):
        self.youtube = YoutubeClient(self.access_token)
        self.itunes = ItunesClient()
        self.genre_cache = {}
    @retry(retries=3, delay=2)
    def _fetch(self, fn):
        return fn()
    def process(self, element):
        logger.info("Fetching playlists from YouTube...")
        playlists = self.youtube.get_playlists()
        logger.info(f"Total playlists fetched: {len(playlists)}")
        for playlist in playlists:
            playlist_name = playlist["snippet"]["title"]
            playlist_id = playlist["id"]
            tracks = self.youtube.get_playlist_tracks(playlist_id)
            logger.info(f"Playlist '{playlist_name}' has {len(tracks)} tracks")
            track_rows = []
            video_ids = []
            for track in tracks:
                video_id = track["snippet"]["resourceId"]["videoId"]
                track_rows.append(track)
                video_ids.append(video_id)
            stats_lookup = {}
            for i in range(0, len(video_ids), 50):
                batch = video_ids[i:i + 50]
                stats_lookup.update(self._fetch(lambda b=batch: self.youtube.get_video_stats(b)))
            for track in track_rows:
                track_title = track["snippet"]["title"]
                video_id = track["snippet"]["resourceId"]["videoId"]
                if track_title in self.genre_cache:
                    genre, artist, country, collection_name, collection_id, track_millis = (self.genre_cache[track_title])
                else:
                    genre, artist, country, collection_name, collection_id, track_millis = (self._fetch(lambda t=track_title: self.itunes.get_genre(t)))
                    self.genre_cache[track_title] = (genre, artist, country, collection_name, collection_id, track_millis)
                view_count, like_count, comment_count = stats_lookup.get(video_id, (0, 0, 0))
                yield {
                    "playlist_name":   playlist_name,
                    "track_title":     track_title,
                    "artist_name":     artist,
                    "video_id":        video_id,
                    "genre":           genre,
                    "country":         country,
                    "collection_name": collection_name,
                    "collection_id":   collection_id,
                    "trackTimeMillis": track_millis,
                    "view_count":      view_count,
                    "like_count":      like_count,
                    "comment_count":   comment_count,
                }
