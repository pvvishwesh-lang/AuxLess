import apache_beam as beam
import requests
import os
import json
import re
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.textio import WriteToText
import csv
import io
from time import sleep


def dict_to_csv_line(record):
    columns = ['playlist_name', 'track_title', 'artist_name', 'video_id', 'genre','country','collection_name','collection_id','trackTimeMillis', 'view_count','like_count','comment_count']
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([record.get(col, "") for col in columns])
    return output.getvalue().strip()


class Authorization_And_Playlistdata:
    def __init__(self,token_uri,client_id,client_secret,redirect_uri,refresh_token):
        self.token_uri=token_uri
        self.client_id=client_id
        self.client_secret=client_secret
        self.redirect_uri=redirect_uri
        self.refresh_token=refresh_token
        
    def exchange_code_for_api(self):
        url=self.token_uri
        data={
        'refresh_token':self.refresh_token,
        'client_id':self.client_id,
        'client_secret':self.client_secret,
        'redirect_uri':self.redirect_uri,
        'grant_type':'refresh_token'
        }
        res=requests.post(url,data=data)
        res.raise_for_status()
        return res.json()['access_token']

    def setup(self):
        self.access_token=self.exchange_code_for_api()

    
    def get_playlists(self):
        url='https://www.googleapis.com/youtube/v3/playlists'
        headers={
        'Authorization':f'Bearer {self.access_token}'
        }
        params={
        'part':'snippet,contentDetails',
        'mine':'true',
        'maxResults':50
        }
        all_playlists = []
        while True:
            res = requests.get(url, headers=headers, params=params)
            res.raise_for_status()
            data = res.json()
            all_playlists.extend(data.get('items', []))
            if 'nextPageToken' in data:
                params['pageToken'] = data['nextPageToken']
            else:
                break
        return all_playlists

    def get_video_stats(self, video_ids):
        url = "https://www.googleapis.com/youtube/v3/videos"
        headers = {
        'Authorization': f'Bearer {self.access_token}'
        }
        params = {
        'part': 'statistics',
        'id': ','.join(video_ids)
        }

        res = requests.get(url, headers=headers, params=params, timeout=10)
        res.raise_for_status()
        data = res.json()

        stats_map={}
        for item in data.get('items',[]):
            vid=item['id']
            stats=item.get('statistics',{})
            stats_map[vid]=(
                int(stats.get('viewCount',0)),
                int(stats.get('likeCount',0)),
                int(stats.get('commentCount',0))
            )
        return stats_map


    def get_playlist_tracks(self,playlist_id):
        url='https://www.googleapis.com/youtube/v3/playlistItems'
        headers={
        'Authorization':f'Bearer {self.access_token}'
        }
        params={
        'part':'snippet,contentDetails',
        'playlistId':playlist_id,
        'maxResults':50
        }
        all_playlists = []
        while True:
            res = requests.get(url, headers=headers, params=params)
            res.raise_for_status()
            data = res.json()
            all_playlists.extend(data.get('items', []))
            if 'nextPageToken' in data:
                params['pageToken'] = data['nextPageToken']
            else:
                break
        return all_playlists
    
    def get_genre(self,track_title,artist_name='',country='',collection_name='',collection_id='',trackTimeMillis=''):
        try:
            query = f"{track_title} {artist_name}".strip()
            url = "https://itunes.apple.com/search"
            params = {
            "term": query,
            "entity": "song",
            "limit": 1
            }
            res = requests.get(url, params=params, timeout=5)
            data = res.json()
            results = data.get("results", [])
            if results:
                song_info=results[0]
                genre=song_info.get("primaryGenreName", "Unknown")
                artist=song_info.get('artistName',artist_name)
                country=song_info.get('country',country)
                collection_name=song_info.get('collectionName',collection_name)
                collection_id=song_info.get('collectionId',collection_id)
                trackTimeMillis=song_info.get('trackTimeMillis',trackTimeMillis)
                return genre,artist,country,collection_name,collection_id,trackTimeMillis
        except Exception as e:
            print(f"iTunes API error: {e}")
        return "Unknown",artist_name,country,collection_name,collection_id,trackTimeMillis
    

class ReadFromAPI(beam.DoFn):
    def __init__(self,refresh_token,token_uri,client_id,client_secret,redirect_uri):
        self.token_uri=token_uri
        self.client_id=client_id
        self.client_secret=client_secret
        self.redirect_uri=redirect_uri
        self.refresh_token=refresh_token

    def setup(self):
        self.Aapd=Authorization_And_Playlistdata(token_uri=self.token_uri,client_id=self.client_id,client_secret=self.client_secret,redirect_uri=self.redirect_uri,refresh_token=self.refresh_token)
        self.Aapd.setup()
        self.genre_cache = {}

    def _retry_request(self,fn,retries=3,delay=2):
        last_exception=None
        for i in range(retries):
            try:
                return fn()
            except Exception as e:
                last_exception=e
                sleep(delay*(2**i))
        raise last_exception
                
        
    def process(self,element):
        playlists=self.Aapd.get_playlists()
        for playlist in playlists:
            playlist_name=playlist['snippet']['title']
            playlist_id=playlist['id']
            tracks=self.Aapd.get_playlist_tracks(playlist_id)
            track_rows=[]
            video_ids=[]
            for track in tracks:
                video_id=track['snippet']['resourceId']['videoId']
                track_rows.append(track)
                video_ids.append(video_id)

            stats_lookup={}

            for i in range(0,len(video_ids),50):
                batch_ids=video_ids[i:i+50]
                stats_lookup.update(self._retry_request(lambda:self.Aapd.get_video_stats(batch_ids)))
                                    
            for track in track_rows:
                track_title=track['snippet']['title']
                video_id=track['snippet']['resourceId']['videoId']
                key = f"{track_title}"
                if key in self.genre_cache:
                    genre, artist_name, country, collection_name, collection_id, trackTimeMillis = self.genre_cache[key]
                else:
                    genre,artist_name,country,collection_name,collection_id,trackTimeMillis=self._retry_request(lambda:self.Aapd.get_genre(track_title))
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


options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = os.environ['PROJECT_ID']
google_cloud_options.job_name = "youtube-pipeline"
google_cloud_options.region='us-central1'
google_cloud_options.staging_location = "gs://youtube-pipeline-staging-bucket/staging"
google_cloud_options.temp_location = "gs://youtube-pipeline-staging-bucket/temp"
options.view_as(StandardOptions).runner = "DataflowRunner"
google_cloud_options.service_account_email='serviceaccountforgithub@main-shade-485500-a0.iam.gserviceaccount.com'

with beam.Pipeline(options=options) as p:
    header = ','.join(['playlist_name', 'track_title', 'artist_name', 'video_id', 'genre','country','collection_name','collection_id','trackTimeMillis', 'view_count','like_count','comment_count'])
    data=(
        p
        |'Seed'>>beam.Create([None])
        |'Read From API'>>beam.ParDo(ReadFromAPI(refresh_token=os.environ['YOUTUBE_REFRESH_TOKEN'],token_uri=os.environ['TOKEN_URI'],client_id=os.environ['CLIENT_ID'],client_secret=os.environ['CLIENT_SECRET'],redirect_uri=os.environ['REDIRECT_URIS']))
        |'ToCSV' >> beam.Map(dict_to_csv_line)
    )
    header_pcoll = (
        p | 'Header' >> beam.Create([header])
    )
    final_csv = (
        (header_pcoll, data) | beam.Flatten()
    )
    final_csv |'WriteToGCS'>> WriteToText(file_path_prefix='gs://youtube-pipeline-staging-bucket/Final_Output',file_name_suffix='.csv',shard_name_template='')


