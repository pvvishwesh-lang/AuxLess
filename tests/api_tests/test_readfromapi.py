import pytest
from backend.pipelines.api.ReadFromAPI import ReadFromAPI


def test_process_yields_records(mock_youtube_client,mock_itunes_client):
  api=ReadFromAPI(access_token='fake')
  api.youtube=mock_youtube_client
  api.itunes=mock_itunes_client
  records=list(api.process(None))
  assert len(records)==1
  rec=records[0]
  assert rec['playlist_name']=="My Playlist"
  assert rec['track_title']=="Song1"
  assert rec['genre']=="Pop"
  assert rec['view_count']==100

def test_process_empty_playlists(monkeypatch,mock_itunes_client):
  class EmptyYoutubeClient:
    def get_playlists(self):
      return []
    api=ReadFromAPI(access_token='fake')
    api.youtube=EmptyYoutubeClient()
    api.itunes=mock_itunes_client
    records=list(api.process(None))
    assert records==[]
    
def test_retry_request_success(monkeypatch):
  api=ReadFromAPI(access_token='fake')
  call_count={'cnt':0}
  def flaky_fn():
    call_count['cnt']+=1
    if call_count['cnt']<3:
      raise Exception('Failed')
    return 'ok'
  result=api._retry_request(flaky_fn,retries=5,delay=0)
  assert result=="ok"
  assert call_count["cnt"]==3

def test_retry_request_failure(monkeypatch):
  api=ReadFromAPI(access_token='fake')
  def always_fail():
    raise Exception('Fail')
  with pytest.raises(Exception) as e:
    api._retry_request(always_fail, retries=2, delay=0)
  assert str(e.value) == "fail"

def test_itunes_unknown(monkeypatch):
  class YoutubeClientMock:
    def get_playlists(self):
      return [{"id":"pl1","snippet":{"title":"pl"}}]
    def get_playlist_tracks(self, pid):
      return [{"snippet":{"title":"t","resourceId":{"videoId":"v"}}}]
    def get_video_stats(self, ids):
      return {"v": (1,2,3)}
  class ItunesUnknown:
    def get_genre(self, title):
      return ("Unknown","","","","","")
  api=ReadFromAPI(access_token="fake")
  api.youtube=YoutubeClientMock()
  api.itunes=ItunesUnknown()
  records=list(api.process(None))
  assert records[0]['genre']=="Unknown"



  













  
    
