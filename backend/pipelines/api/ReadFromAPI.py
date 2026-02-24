import apache_beam as beam
from time import sleep
from backend.pipelines.api.youtube_client import YoutubeClient
from backend.pipelines.api.itunes_client import ItunesClient
import logging
from backend.pipelines.api.beam_extensions import ValidatingDoFn
from backend.pipelines.api.structured_logger import get_logger
from backend.pipelines.api.retry_utils import retry

logging.basicConfig(level=logging.INFO)

class ReadFromAPI(beam.DoFn):
    def __init__(self,access_token):
        self.access_token=access_token
        self.genre_cache = {}

    def setup(self):
        self.youtube = YoutubeClient(self.access_token)
        self.itunes = ItunesClient()
    
    @retry(retries=3, delay=2)
    def _retry_request(self,fn):
        return fn()
                
        
    def process(self,element):
        logging.info("Fetching playlists from YouTube...")
        playlists=self.youtube.get_playlists()
        logging.info(f"Total playlists fetched: {len(playlists)}")
        for playlist in playlists:
            playlist_name=playlist['snippet']['title']
            playlist_id=playlist['id']
            tracks=self.youtube.get_playlist_tracks(playlist_id)
            logging.info(f"Playlist '{playlist_name}' has {len(tracks)} tracks")
            track_rows=[]
            video_ids=[]
            for track in tracks:
                video_id=track['snippet']['resourceId']['videoId']
                track_rows.append(track)
                video_ids.append(video_id)

            stats_lookup={}

            for i in range(0,len(video_ids),50):
                batch_ids=video_ids[i:i+50]
                stats_lookup.update(self._retry_request(lambda:self.youtube.get_video_stats(batch_ids)))
                                    
            for track in track_rows:
                track_title=track['snippet']['title']
                video_id=track['snippet']['resourceId']['videoId']
                key = f"{track_title}"
                if key in self.genre_cache:
                    genre, artist_name, country, collection_name, collection_id, trackTimeMillis = self.genre_cache[key]
                else:
                    genre,artist_name,country,collection_name,collection_id,trackTimeMillis=self._retry_request(lambda:self.itunes.get_genre(track_title))
                    self.genre_cache[key] = (genre, artist_name, country, collection_name, collection_id, trackTimeMillis)
                view_count,like_count,comment_count=stats_lookup.get(video_id,(0,0,0))
                yield {
                    'playlist_name':playlist_name,
                    'track_title':track_title,
                    'artist_name':artist_name,
                    'video_id':video_id,
                    'genre':genre,
                    'country':country,
                    'collection_name':collection_name,
                    'collection_id':collection_id,
                    'trackTimeMillis':trackTimeMillis,
                    'view_count': view_count,
                    'like_count': like_count,
                    'comment_count': comment_count
                }
