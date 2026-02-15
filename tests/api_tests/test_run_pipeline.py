import pytest
import apache_beam as beam
from backend.pipelines.run_all_users import run_for_session
"""
Forces Apache Beam to use DirectRunner during tests
instead of trying to use Dataflow.
"""
@pytest.fixture(autouse=True)
def force_direct_runner(monkeypatch):
    monkeypatch.setattr("backend.pipelines.Api_Puller.beam.Pipeline",lambda *a, **k: beam.Pipeline(runner="DirectRunner"))


class FakeFS:
    def get_session_users(self, session_id):
        return [("u1", "t1")]
    def get_session_status(self, session_id):
        return "running"
    def update_session_status(self, session_id, status):
        self.status = status
        
def test_run_pipeline_happy_path(mocker):
    mocker.patch("backend.pipelines.run_all_users.FirestoreClient",return_value=FakeFS())
    mocker.patch("backend.pipelines.run_all_users.run_pipeline_for_user")
    mocker.patch("backend.pipelines.run_all_users.write_output_to_firestore")
    run_for_session("test_session")
    assert True
