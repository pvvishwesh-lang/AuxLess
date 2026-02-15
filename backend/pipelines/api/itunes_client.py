import requests

class ItunesClient:
        
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
