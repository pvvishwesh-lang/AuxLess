from backend.pipelines.Api_Puller import run_pipeline_for_user
import apache_beam as beam
import os
from backend.pipelines.api.auth import GoogleAuthClient
import pytest

os.environ["TOKEN_URI"]="test"
os.environ["CLIENT_ID"]="test"
os.environ["CLIENT_SECRET"]="test"
os.environ["REFRESH_TOKEN_URI"]="test"
os.environ["REDIRECT_URIS"]="test"

@pytest.fixture(autouse=True)
def mock_auth(monkeypatch):
    class FakeAuth:
        def __init__(self, *args, **kwargs):
            pass
        def get_access_token(self):
            return "fake_access_token"

    monkeypatch.setattr("backend.pipelines.api.auth.GoogleAuthClient",lambda *a, **k: FakeAuth())

def test_pipeline_for_multiple_users(monkeypatch):
  class FakeDoFn:
    def process(self,element):
      for i in range(3):
        yield {
                    'playlist_name':f'p{i}','track_title':f't{i}','artist_name':'a',
                    'video_id':f'v{i}','genre':'g','country':'c','collection_name':'c1',
                    'collection_id':i,'trackTimeMillis':123,'view_count':i,
                    'like_count':i*2,'comment_count':i*3
                }
  monkeypatch.setattr("backend.pipelines.api.ReadFromAPI.ReadFromAPI", lambda token: FakeDoFn())
  monkeypatch.setattr("apache_beam.io.textio.WriteToText", lambda *a, **kw: beam.Map(lambda x: x))
  run_pipeline_for_user("user1","fake_token","prefix/","sess1")
