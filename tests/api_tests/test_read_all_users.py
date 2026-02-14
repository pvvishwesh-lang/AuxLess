from backend.pipelines.run_all_users import run_for_session

def test_run_for_session_with_no_users(monkeypatch):
  class FakeFS:
    def get_session_users(self,session_id):
      return []
    def get_session_status(self,session_id):
      return 'running'
    def update_session_status(self,session_id,status):
      self.status = status
  monkeypatch.setattr("backend.pipelines.runallusers.FirestoreClient", lambda p,d: FakeFS())
  import pytest
  with pytest.raises(RuntimeError):
    run_for_session("sess1")

def test_run_for_session_skips_if_not_running(monkeypatch):
  class FakeFS:
    def get_session_users(self,session_id):
      return [("u1","t1")]
    def get_session_status(self,session_id):
      return 'done'
    def update_session_status(self,session_id,status):
      self.status = status
  monkeypatch.setattr("backend.pipelines.runallusers.FirestoreClient", lambda p,d: FakeFS())
  run_for_session("sess1")

  
