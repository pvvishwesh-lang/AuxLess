import apache_beam as beam
import requests
import os
import json
import re


class Authorization_And_Playlistdata(beam.DoFn):
    def __init__(self,token_uri,client_id,client_secret,redirect_uri,access_token):
        self.token_uri=token_uri
        self.client_id=client_id
        self.client_secret=client_secret
        self.redirect_uri=redirect_uri
        self.access_token=access_token
    def exchange_code_for_api(self,code):
        url=self.token_uri
        data={
        'code':code,
        'client_id':self.client_id,
        'client_secret':self.client_secret,
        'redirect_uri':self.redirect_uri,
        'grant_type':'authorization_code'
        }
        res=requests.post(url,data=data)
        res.raise_for_status()
        return res.json()

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
    
    def get_genre(self,track_title,artist_name=''):
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
                return genre,artist
        except Exception as e:
            print(f"iTunes API error: {e}")
        return "Unknown",artist_name
    
    
    def clean_title(self,title):
        title = re.sub(r'\(.*?\)', '', title)
        title = re.sub(r'\[.*?\]', '', title)
        title = re.sub(r'feat\. .*', '', title, flags=re.IGNORECASE)
        return title.strip()
    
class ReadFromAPI(beam.DoFn):
    def __init__(self,access_token,token_uri,client_id,client_secret,redirect_uri):
        self.token_uri=token_uri
        self.client_id=client_id
        self.client_secret=client_secret
        self.redirect_uri=redirect_uri
        self.access_token=access_token
    
    def process(self,element):
        Aapd=Authorization_And_Playlistdata(token_uri=self.token_uri,client_id=self.client_id,client_secret=self.client_secret,redirect_uri=self.redirect_uri,access_token=self.access_token)
        playlists=Aapd.get_playlists()
        for playlist in playlists:
            playlist_name=playlist['snippet']['title']
            playlist_id=playlist['id']
            tracks=Aapd.get_playlist_tracks(playlist_id)
            for track in tracks:
                track_title=track['snippet']['title']
                genre,artist_name=Aapd.get_genre(track_title)
                yield {
                    'playlist_name':playlist_name,
                    'track_title':track_title,
                    'artist_name':artist_name,
                    'video_id':track['snippet']['resourceId']['videoId'],
                    'genre':genre
                }

with beam.Pipeline() as p:
    data=(
        p
        |'Seed'>>beam.Create([None])
        |'Read From API'>>beam.ParDo(ReadFromAPI(access_token=os.environ['YOUTUBE_REFRESH_TOKEN'],token_uri=os.environ['TOKEN_URI'],client_id=os.environ['CLIENT_ID'],client_secret=os.environ['CLIENT_SECRET'],redirect_uri=os.environ['REDIRECT_URIS']))
        |'Print'>>beam.Map(print)
    )


