"""
Tests for DVC integration.

These tests verify the DVC configuration files and the dvc_setup helper
without invoking the Beam pipeline or requiring GCP credentials.
"""

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def params(tmp_path, monkeypatch):
    """Load params.yaml with env-var placeholders substituted."""
    monkeypatch.setenv("GCP_PROJECT_ID",       "test-project")
    monkeypatch.setenv("GCP_REGION",           "us-central1")
    monkeypatch.setenv("GCS_TEMP_LOCATION",    "gs://test/temp")
    monkeypatch.setenv("GCS_STAGING_LOCATION", "gs://test/staging")
    monkeypatch.setenv("PUBSUB_TOPIC",         "projects/test-project/topics/test")
    monkeypatch.setenv("GCS_RAW_EVENTS_PATH",  "gs://test/raw")
    monkeypatch.setenv("BQ_TABLE",             "test-project:dataset.table")
    monkeypatch.setenv("FIRESTORE_DATABASE",   "test-db")

    raw = Path("backend/pipelines/params.yaml").read_text()  # ✅ fixed
    import os, re
    resolved = re.sub(
        r"\$\{(\w+)\}",
        lambda m: os.environ.get(m.group(1), m.group(0)),
        raw,
    )
    return yaml.safe_load(resolved)


@pytest.fixture()
def dvc_yaml():
    return yaml.safe_load(Path("backend/pipelines/dvc.yaml").read_text())  # ✅ fixed


# ---------------------------------------------------------------------------
# dvc.yaml structure tests
# ---------------------------------------------------------------------------

class TestDvcYaml:
    def test_file_exists(self):
        assert Path("backend/pipelines/dvc.yaml").exists(), "dvc.yaml must exist"  # ✅ fixed

    def test_has_streaming_pipeline_stage(self, dvc_yaml):
        assert "streaming_pipeline" in dvc_yaml["stages"]

    def test_stage_has_cmd(self, dvc_yaml):
        stage = dvc_yaml["stages"]["streaming_pipeline"]
        assert "cmd" in stage
        assert "run_pipeline" in stage["cmd"]

    def test_stage_has_deps(self, dvc_yaml):
        stage = dvc_yaml["stages"]["streaming_pipeline"]
        assert "deps" in stage
        deps = stage["deps"]
        assert any("run_pipeline" in d for d in deps)
        assert any("feedback_processor" in d for d in deps)

    def test_stage_has_params(self, dvc_yaml):
        stage = dvc_yaml["stages"]["streaming_pipeline"]
        assert "params" in stage

    def test_stage_has_metrics(self, dvc_yaml):
        stage = dvc_yaml["stages"]["streaming_pipeline"]
        assert "metrics" in stage

    def test_metrics_not_cached(self, dvc_yaml):
        stage = dvc_yaml["stages"]["streaming_pipeline"]
        for entry in stage["metrics"]:
            for _path, cfg in entry.items():
                assert cfg.get("cache") is False


# ---------------------------------------------------------------------------
# params.yaml structure tests
# ---------------------------------------------------------------------------

class TestParamsYaml:
    def test_file_exists(self):
        assert Path("backend/pipelines/params.yaml").exists(), "params.yaml must exist"  # ✅ fixed

    def test_pipeline_section_exists(self, params):
        assert "pipeline" in params

    def test_window_size_is_positive_int(self, params):
        ws = params["pipeline"]["window_size_seconds"]
        assert isinstance(ws, int) and ws > 0

    def test_runner_is_dataflow(self, params):
        assert params["pipeline"]["runner"] == "DataflowRunner"

    def test_gcp_section_exists(self, params):
        assert "gcp" in params

    def test_gcp_fields_resolved(self, params):
        gcp = params["gcp"]
        for key in ("project_id", "region", "temp_location", "staging_location"):
            assert key in gcp
            assert "${" not in str(gcp[key]), f"Unresolved placeholder in gcp.{key}"

    def test_storage_section_exists(self, params):
        assert "storage" in params


# ---------------------------------------------------------------------------
# .dvcignore tests
# ---------------------------------------------------------------------------

class TestDvcIgnore:
    def test_file_exists(self):
        assert Path(".dvcignore").exists(), ".dvcignore must exist"

    def test_ignores_jsonl(self):
        content = Path(".dvcignore").read_text()
        assert "*.jsonl" in content

    def test_ignores_pycache(self):
        content = Path(".dvcignore").read_text()
        assert "__pycache__" in content


# ---------------------------------------------------------------------------
# dvc_setup.py helper tests (subprocess is mocked — no DVC required)
# ---------------------------------------------------------------------------

class TestDvcSetupHelper:
    def test_ensure_metrics_dir_creates_dir(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        import importlib, sys
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # ✅ already correct
        from backend.pipelines import dvc_setup
        importlib.reload(dvc_setup)

        dvc_setup.ensure_metrics_dir()

        assert (tmp_path / "metrics").is_dir()
        assert (tmp_path / "metrics" / "pipeline_metrics.json").exists()

    def test_metrics_stub_is_valid_json(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        import importlib, sys
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # ✅ fixed
        from backend.pipelines import dvc_setup                        # ✅ fixed
        importlib.reload(dvc_setup)

        dvc_setup.ensure_metrics_dir()
        data = json.loads((tmp_path / "metrics" / "pipeline_metrics.json").read_text())
        assert "window_size_seconds" in data

    def test_init_dvc_skips_if_already_initialised(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / ".dvc").mkdir()

        import importlib, sys
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # ✅ fixed
        from backend.pipelines import dvc_setup                        # ✅ fixed
        importlib.reload(dvc_setup)

        with patch("backend.pipelines.dvc_setup._run") as mock_run:
            dvc_setup.init_dvc()
            mock_run.assert_not_called()

    def test_repro_calls_dvc_repro(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        import importlib, sys
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # ✅ fixed
        from backend.pipelines import dvc_setup                        # ✅ fixed
        importlib.reload(dvc_setup)

        with patch("backend.pipelines.dvc_setup._run") as mock_run:
            dvc_setup.repro()
            calls = [c.args[0] for c in mock_run.call_args_list]
            assert ["dvc", "repro"] in calls

    def test_show_metrics_calls_dvc_metrics(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        import importlib, sys
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # ✅ fixed
        from backend.pipelines import dvc_setup                        # ✅ fixed
        importlib.reload(dvc_setup)

        with patch("backend.pipelines.dvc_setup._run") as mock_run:
            dvc_setup.show_metrics()
            calls = [c.args[0] for c in mock_run.call_args_list]
            assert ["dvc", "metrics", "show"] in calls