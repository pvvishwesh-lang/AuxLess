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

def test_run_pipeline_happy_path(mocker):
    mocker.patch("backend.pipelines.run_all_users.get_session_data", return_value={"users": [1, 2]})
    mocker.patch("backend.pipelines.run_all_users.write_output_to_firestore")
    run_for_session("test_session")
    assert True
