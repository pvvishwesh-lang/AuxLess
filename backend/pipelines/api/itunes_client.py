import requests
import logging

logger = logging.getLogger(__name__)

_EMPTY = ("Unknown", "", "", "", "", "")


class ItunesClient:
    BASE_URL = "https://itunes.apple.com/search"

    def get_genre(
        self,
        track_title: str,
        artist_name: str = "",
        country: str = "",
        collection_name: str = "",
        collection_id: str = "",
        track_time_millis: str = ""
    ) -> tuple:
        query = f"{track_title} {artist_name}".strip()
        try:
            res = requests.get(
                self.BASE_URL,
                params={"term": query, "entity": "song", "limit": 1},
                timeout=5
            )
            res.raise_for_status()
            results = res.json().get("results", [])
            if not results:
                return "Unknown", artist_name, country, collection_name, collection_id, track_time_millis
            song = results[0]
            return (
                song.get("primaryGenreName", "Unknown"),
                song.get("artistName",       artist_name),
                song.get("country",          country),
                song.get("collectionName",   collection_name),
                song.get("collectionId",     collection_id),
                song.get("trackTimeMillis",  track_time_millis)
            )
        except requests.HTTPError as e:
            raise
        except Exception as e:
            logger.error(f"iTunes unexpected error for '{track_title}': {e}")
            return "Unknown", artist_name, country, collection_name, collection_id, track_time_millis
