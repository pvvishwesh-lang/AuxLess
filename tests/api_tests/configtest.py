'''
Used to test the various custom packages present in the Pipeline, such as youtube client,itunes client,firestore client 
and gcs client using Monkeypatch and pytest
'''
import pytest
from unittest.mock import MagicMock

'''
Test for youtube client
'''
@pytest.fixture
def mock_youtube_client(monkeypatch):
  class FakeYoutubeClient:
    def get_playlists(self):
      return [{"id": "pl1", "snippet": {"title": "My Playlist"}}]
    def get_playlist_tracks(self,playlist_id):
      return [{"snippet": {"title": "Song1", "resourceId": {"videoId": "vid1"}}}]
    def get_video_stats(self,video_ids):
      return {"vid1": (100, 10, 1)}
      
  monkeypatch.setattr("backend.pipelines.api.ReadFromAPI.YoutubeClient", lambda token: FakeYoutubeClient())
  return FakeYoutubeClient()

'''
Test for itunes_client
'''
@pytest.fixture
def mock_itunes_client(monkeypatch):
  class FakeItunesClient:
    def get_genre(self,track_title):
      return ("Pop", "Artist1", "US", "Album1", 123, 180000)
  monkeypatch.setattr("backend.pipelines.api.ReadFromAPI.ItunesClient", lambda token: FakeItunesClient())
  return FakeItunesClient()

'''
Test for firestore_client
'''
@pytest.fixture
def mock_firestore_client(monkeypatch):
  class FakeFirestoreClient:
    status = None
    def get_session_users(self,session_id):
      return [("user1", "token1")]
    def get_session_status(self,session_id):
      return "running"
    def update_session_status(self,session_id):
      self.status = status
  monkeypatch.setattr("backend.pipelines.run_all_users.FirestoreClient", lambda token: FakeFirestoreClient())
  return FakeFirestoreClient()

'''
Test for gcs storange
'''
@pytest.fixture
def mock_gcs_client(monkeypatch):
  class FakeBlob:
    def __init__(self,name):
      self.name=name
      self.content=''
    def download_as_text(self):
      return self.content
    def upload_from_string(self):
      self.content = data
    def delete(self):
      self.content=""
  class FakeBucket:
    def __init__(self):
      self.blobs = [FakeBlob("file1.csv"), FakeBlob("file2.csv")]
    def list_blobs(self):
      return self.blobs
    def blob(self,name):
      return FakeBlob(blob)
  class FakeStorageClient:
    def bucket(self,name):
      return FakeBucket()
  monkeypatch.setattr("backend.pipelines.run_all_users.storage.Client", lambda token: FakeStorageClient())
  return FakeStorageClient()
















    













    




