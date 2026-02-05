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
