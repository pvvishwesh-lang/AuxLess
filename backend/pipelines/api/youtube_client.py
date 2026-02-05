import requests

class YoutubeClient:
    def __inti__(self,access_token):
        self.headers={'Authorization':f'Bearer {access_token}'}

    def get_playlists(self):
        url='https://www.googleapis.com/youtube/v3/playlists'
        params={
        'part':'snippet,contentDetails',
        'mine':'true',
        'maxResults':50
        }
        all_playlists = []
        while True:
            res = requests.get(url, headers=self.headers, params=params)
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
        params={
        'part':'snippet,contentDetails',
        'playlistId':playlist_id,
        'maxResults':50
        }
        all_playlists = []
        while True:
            res = requests.get(url, headers=self.headers, params=params)
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
        params = {
        'part': 'statistics',
        'id': ','.join(video_ids)
        }
        res = requests.get(url, headers=self.headers, params=params, timeout=10)
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
