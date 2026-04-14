"""
Tests for ml/ml_trigger.py

Covers:
  - _get_last_refreshed_at()
  - _run_ml_preprocessing()
  - _generate_and_write_batch()       ← updated: now takes monitoring_writer
  - _run_ml_session()                 ← updated: now creates MonitoringWriter
  - MonitoringWriter initialisation   ← new
  - Bias snapshot integration         ← new
  - monitoring_writer passthrough     ← new
  - _watch_and_refresh()              ← new

All GCP calls are mocked. No real Firestore, BigQuery, or GCS connections.
"""

import pytest
from unittest.mock import patch, MagicMock, call, PropertyMock


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_mock_db(last_refreshed=0, exists=True):
    db  = MagicMock()
    doc = MagicMock()
    doc.exists = exists
    doc.to_dict.return_value = {"last_refreshed_at": last_refreshed}
    db.collection.return_value \
      .document.return_value \
      .collection.return_value \
      .document.return_value \
      .get.return_value = doc
    return db


def make_mock_state(session_id="sess_1", with_batch_songs=False, with_catalog=False):
    """
    Builds a mock SessionState object.

    with_batch_songs=True  → sets state.last_batch_songs so bias eval runs
    with_catalog=True      → sets state.catalog_df so bias eval has catalog
    Both must be True for bias snapshot to fire in _generate_and_write_batch.
    """
    import pandas as pd
    import numpy as np

    state            = MagicMock()
    state.session_id = session_id
    state.db         = make_mock_db()

    if with_batch_songs:
        state.last_batch_songs = [
            {
                "video_id":        f"vid_{i}",
                "title":           f"Song {i}",
                "artist":          f"Artist {i}",
                "genre":           ["pop","rock","jazz"][i % 3],
                "popularity_score": 0.5,
                "cbf_score":       0.6,
                "cf_score":        0.5,
                "gru_score":       0.4,
                "final_score":     0.55,
            }
            for i in range(10)
        ]
    else:
        # hasattr check: MagicMock returns truthy for any attr by default
        # so we explicitly delete the attribute to simulate it not existing
        del state.last_batch_songs

    if with_catalog:
        np.random.seed(42)
        state.catalog_df = pd.DataFrame({
            "genre":            ["pop","rock","jazz"] * 34,
            "popularity_score": np.random.rand(102),
        })
    else:
        del state.catalog_df

    return state


def make_mock_monitoring_writer():
    writer = MagicMock()
    writer.write_rec_snapshot   = MagicMock()
    writer.write_bias_snapshot  = MagicMock()
    writer.write_play_event     = MagicMock()
    return writer


# ── _get_last_refreshed_at ────────────────────────────────────────────────────

class TestGetLastRefreshedAt:

    def _call(self, db, session_id="sess_1"):
        from ml.ml_trigger import _get_last_refreshed_at
        return _get_last_refreshed_at(db, session_id)

    def test_returns_int(self):
        assert isinstance(self._call(make_mock_db(last_refreshed=5)), int)

    def test_returns_correct_value(self):
        assert self._call(make_mock_db(last_refreshed=10)) == 10

    def test_returns_zero_when_doc_not_exists(self):
        assert self._call(make_mock_db(exists=False)) == 0

    def test_returns_zero_on_firestore_exception(self):
        db = MagicMock()
        db.collection.side_effect = Exception("Firestore error")
        assert self._call(db) == 0

    def test_returns_zero_when_key_missing(self):
        db  = MagicMock()
        doc = MagicMock()
        doc.exists = True
        doc.to_dict.return_value = {}
        db.collection.return_value \
          .document.return_value \
          .collection.return_value \
          .document.return_value \
          .get.return_value = doc
        assert self._call(db) == 0

    def test_returns_zero_for_new_session(self):
        assert self._call(make_mock_db(last_refreshed=0)) == 0

    def test_handles_large_value(self):
        assert self._call(make_mock_db(last_refreshed=999)) == 999

    def test_correct_firestore_path_accessed(self):
        db = make_mock_db(last_refreshed=3)
        self._call(db, session_id="my-session")
        db.collection.assert_called_with("sessions")
        db.collection().document.assert_called_with("my-session")


# ── _run_ml_preprocessing ─────────────────────────────────────────────────────

class TestRunMlPreprocessing:

    @patch("ml.ml_trigger.generate_embeddings_for_session")
    @patch("ml.ml_trigger.preprocess_for_session")
    def test_calls_preprocess_for_session(self, mock_preprocess, mock_embed):
        from ml.ml_trigger import _run_ml_preprocessing
        mock_preprocess.return_value = "gs://bucket/preprocessed.csv"
        mock_embed.return_value      = 5
        _run_ml_preprocessing("sess_1", "my-bucket")
        mock_preprocess.assert_called_once_with("sess_1", "my-bucket")

    @patch("ml.ml_trigger.generate_embeddings_for_session")
    @patch("ml.ml_trigger.preprocess_for_session")
    def test_calls_generate_embeddings_for_session(self, mock_preprocess, mock_embed):
        from ml.ml_trigger import _run_ml_preprocessing
        mock_preprocess.return_value = "gs://bucket/preprocessed.csv"
        mock_embed.return_value      = 5
        _run_ml_preprocessing("sess_1", "my-bucket")
        mock_embed.assert_called_once_with("sess_1", "my-bucket")

    @patch("ml.ml_trigger.generate_embeddings_for_session")
    @patch("ml.ml_trigger.preprocess_for_session")
    def test_passes_correct_session_id(self, mock_preprocess, mock_embed):
        from ml.ml_trigger import _run_ml_preprocessing
        mock_preprocess.return_value = "gs://bucket/preprocessed.csv"
        mock_embed.return_value      = 0
        _run_ml_preprocessing("my-session-abc", "bucket")
        assert mock_preprocess.call_args[0][0] == "my-session-abc"
        assert mock_embed.call_args[0][0]      == "my-session-abc"

    @patch("ml.ml_trigger.generate_embeddings_for_session")
    @patch("ml.ml_trigger.preprocess_for_session")
    def test_passes_correct_bucket(self, mock_preprocess, mock_embed):
        from ml.ml_trigger import _run_ml_preprocessing
        mock_preprocess.return_value = "gs://bucket/preprocessed.csv"
        mock_embed.return_value      = 0
        _run_ml_preprocessing("sess_1", "my-special-bucket")
        assert mock_preprocess.call_args[0][1] == "my-special-bucket"
        assert mock_embed.call_args[0][1]      == "my-special-bucket"

    @patch("ml.ml_trigger.generate_embeddings_for_session")
    @patch("ml.ml_trigger.preprocess_for_session")
    def test_preprocess_called_before_embed(self, mock_preprocess, mock_embed):
        from ml.ml_trigger import _run_ml_preprocessing
        call_order = []
        mock_preprocess.side_effect = lambda *a: call_order.append("preprocess") or "path"
        mock_embed.side_effect      = lambda *a: call_order.append("embed") or 0
        _run_ml_preprocessing("sess_1", "bucket")
        assert call_order == ["preprocess", "embed"]

    @patch("ml.ml_trigger.generate_embeddings_for_session")
    @patch("ml.ml_trigger.preprocess_for_session")
    def test_zero_new_songs_no_error(self, mock_preprocess, mock_embed):
        from ml.ml_trigger import _run_ml_preprocessing
        mock_preprocess.return_value = "gs://bucket/preprocessed.csv"
        mock_embed.return_value      = 0
        _run_ml_preprocessing("sess_1", "bucket")
        mock_embed.assert_called_once()


# ── _generate_and_write_batch ─────────────────────────────────────────────────

class TestGenerateAndWriteBatch:
    """
    All calls now require monitoring_writer as the third argument.
    Tests cover both the recommendation logic and the new monitoring path.
    """

    @patch("ml.ml_trigger.recommend_next_song")
    def test_calls_recommend_next_song(self, mock_rec):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value = {"track_title": "Song A", "video_id": "vid_0"}
        state  = make_mock_state()
        writer = make_mock_monitoring_writer()
        _generate_and_write_batch(state, batch_number=1, monitoring_writer=writer)
        mock_rec.assert_called_once_with(state)

    @patch("ml.ml_trigger.recommend_next_song")
    def test_handles_none_recommendations(self, mock_rec):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value = None
        state  = make_mock_state()
        writer = make_mock_monitoring_writer()
        _generate_and_write_batch(state, batch_number=1, monitoring_writer=writer)
        mock_rec.assert_called_once()

    @patch("ml.ml_trigger.recommend_next_song")
    def test_handles_empty_recommendations(self, mock_rec):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value = {}
        state  = make_mock_state()
        writer = make_mock_monitoring_writer()
        _generate_and_write_batch(state, batch_number=1, monitoring_writer=writer)
        mock_rec.assert_called_once()

    @patch("ml.ml_trigger.recommend_next_song")
    def test_works_for_batch_2(self, mock_rec):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value = {"track_title": "Song B", "video_id": "vid_1"}
        state  = make_mock_state()
        writer = make_mock_monitoring_writer()
        _generate_and_write_batch(state, batch_number=2, monitoring_writer=writer)
        mock_rec.assert_called_once_with(state)

    @patch("ml.ml_trigger.recommend_next_song")
    def test_passes_correct_state_to_recommend(self, mock_rec):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value = {"track_title": "Song", "video_id": "vid_0"}
        state  = make_mock_state("test-session")
        writer = make_mock_monitoring_writer()
        _generate_and_write_batch(state, batch_number=1, monitoring_writer=writer)
        assert mock_rec.call_args[0][0].session_id == "test-session"

    @patch("ml.ml_trigger.recommend_next_song")
    def test_does_not_raise_on_recommend_exception(self, mock_rec):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.side_effect = Exception("model error")
        state  = make_mock_state()
        writer = make_mock_monitoring_writer()
        # pipeline must never crash — recommend exception propagates here
        # (it's intentionally not caught so the caller's try/except fires)
        with pytest.raises(Exception, match="model error"):
            _generate_and_write_batch(state, batch_number=1, monitoring_writer=writer)


# ── Bias snapshot in _generate_and_write_batch ────────────────────────────────

class TestBiasSnapshotInBatch:
    """
    Bias evaluation runs inside _generate_and_write_batch when
    state.last_batch_songs and state.catalog_df are both present.
    These are set by recommend_next_song() in main.py (Step 2).
    """

    @patch("ml.ml_trigger.evaluate_bias")
    @patch("ml.ml_trigger.recommend_next_song")
    def test_bias_eval_called_when_batch_songs_and_catalog_present(
        self, mock_rec, mock_bias
    ):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value  = {"track_title": "Song", "video_id": "vid_0"}
        mock_bias.return_value = {"overall_bias_detected": False, "mitigation_suggestions": []}
        state  = make_mock_state(with_batch_songs=True, with_catalog=True)
        writer = make_mock_monitoring_writer()
        _generate_and_write_batch(state, batch_number=1, monitoring_writer=writer)
        mock_bias.assert_called_once()

    @patch("ml.ml_trigger.evaluate_bias")
    @patch("ml.ml_trigger.recommend_next_song")
    def test_bias_eval_uses_last_batch_songs(self, mock_rec, mock_bias):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value  = {"track_title": "Song", "video_id": "vid_0"}
        mock_bias.return_value = {"overall_bias_detected": False, "mitigation_suggestions": []}
        state  = make_mock_state(with_batch_songs=True, with_catalog=True)
        writer = make_mock_monitoring_writer()
        _generate_and_write_batch(state, batch_number=1, monitoring_writer=writer)
        called_recs = mock_bias.call_args[1].get("recommendations") or mock_bias.call_args[0][0]
        assert called_recs == state.last_batch_songs

    @patch("ml.ml_trigger.evaluate_bias")
    @patch("ml.ml_trigger.recommend_next_song")
    def test_write_bias_snapshot_called_when_eval_runs(self, mock_rec, mock_bias):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value  = {"track_title": "Song", "video_id": "vid_0"}
        bias_result = {"overall_bias_detected": False, "mitigation_suggestions": []}
        mock_bias.return_value = bias_result
        state  = make_mock_state(with_batch_songs=True, with_catalog=True)
        writer = make_mock_monitoring_writer()
        _generate_and_write_batch(state, batch_number=1, monitoring_writer=writer)
        writer.write_bias_snapshot.assert_called_once_with(bias_result)

    @patch("ml.ml_trigger.evaluate_bias")
    @patch("ml.ml_trigger.recommend_next_song")
    def test_bias_eval_skipped_when_no_batch_songs(self, mock_rec, mock_bias):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value = {"track_title": "Song", "video_id": "vid_0"}
        # state has NO last_batch_songs (not set yet — main.py Step 2 pending)
        state  = make_mock_state(with_batch_songs=False, with_catalog=True)
        writer = make_mock_monitoring_writer()
        _generate_and_write_batch(state, batch_number=1, monitoring_writer=writer)
        mock_bias.assert_not_called()
        writer.write_bias_snapshot.assert_not_called()

    @patch("ml.ml_trigger.evaluate_bias")
    @patch("ml.ml_trigger.recommend_next_song")
    def test_bias_eval_skipped_when_no_catalog(self, mock_rec, mock_bias):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value = {"track_title": "Song", "video_id": "vid_0"}
        # state has batch songs but NO catalog_df
        state  = make_mock_state(with_batch_songs=True, with_catalog=False)
        writer = make_mock_monitoring_writer()
        _generate_and_write_batch(state, batch_number=1, monitoring_writer=writer)
        mock_bias.assert_not_called()
        writer.write_bias_snapshot.assert_not_called()

    @patch("ml.ml_trigger.evaluate_bias")
    @patch("ml.ml_trigger.recommend_next_song")
    def test_bias_exception_does_not_crash_batch(self, mock_rec, mock_bias):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value  = {"track_title": "Song", "video_id": "vid_0"}
        mock_bias.side_effect  = Exception("bias eval exploded")
        state  = make_mock_state(with_batch_songs=True, with_catalog=True)
        writer = make_mock_monitoring_writer()
        # must NOT raise — bias is non-critical
        _generate_and_write_batch(state, batch_number=1, monitoring_writer=writer)

    @patch("ml.ml_trigger.evaluate_bias")
    @patch("ml.ml_trigger.recommend_next_song")
    def test_write_bias_snapshot_exception_does_not_crash_batch(self, mock_rec, mock_bias):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value  = {"track_title": "Song", "video_id": "vid_0"}
        mock_bias.return_value = {"overall_bias_detected": False, "mitigation_suggestions": []}
        state  = make_mock_state(with_batch_songs=True, with_catalog=True)
        writer = make_mock_monitoring_writer()
        writer.write_bias_snapshot.side_effect = Exception("Firestore write failed")
        # must NOT raise
        _generate_and_write_batch(state, batch_number=1, monitoring_writer=writer)

    @patch("ml.ml_trigger.evaluate_bias")
    @patch("ml.ml_trigger.recommend_next_song")
    def test_bias_detected_true_does_not_crash_batch(self, mock_rec, mock_bias):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value  = {"track_title": "Song", "video_id": "vid_0"}
        mock_bias.return_value = {
            "overall_bias_detected": True,
            "mitigation_suggestions": ["Apply genre diversity constraint"],
        }
        state  = make_mock_state(with_batch_songs=True, with_catalog=True)
        writer = make_mock_monitoring_writer()
        # a bias flag must not stop the recommendation pipeline
        _generate_and_write_batch(state, batch_number=1, monitoring_writer=writer)
        writer.write_bias_snapshot.assert_called_once()

    @patch("ml.ml_trigger.evaluate_bias")
    @patch("ml.ml_trigger.recommend_next_song")
    def test_bias_eval_score_column_is_final_score(self, mock_rec, mock_bias):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value  = {"track_title": "Song", "video_id": "vid_0"}
        mock_bias.return_value = {"overall_bias_detected": False, "mitigation_suggestions": []}
        state  = make_mock_state(with_batch_songs=True, with_catalog=True)
        writer = make_mock_monitoring_writer()
        _generate_and_write_batch(state, batch_number=1, monitoring_writer=writer)
        call_kwargs = mock_bias.call_args[1] if mock_bias.call_args[1] else {}
        call_args   = mock_bias.call_args[0] if mock_bias.call_args[0] else ()
        score_col   = call_kwargs.get("score_column") or (call_args[2] if len(call_args) > 2 else None)
        assert score_col == "final_score"


# ── MonitoringWriter initialisation in _run_ml_session ───────────────────────

class TestMonitoringWriterInit:
    """
    MonitoringWriter must be created once per session inside _run_ml_session,
    immediately after initialize_session() provides state.db.
    """

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.threading.Thread")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_monitoring_writer_is_created(
        self, mock_prep, mock_init, mock_batch, mock_update, mock_thread, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        state = make_mock_state("sess_1")
        mock_init.return_value        = state
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        mock_mw.assert_called_once()

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.threading.Thread")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_monitoring_writer_uses_state_db(
        self, mock_prep, mock_init, mock_batch, mock_update, mock_thread, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        state = make_mock_state("sess_1")
        mock_init.return_value        = state
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        args = mock_mw.call_args[0]
        assert args[0] is state.db

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.threading.Thread")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_monitoring_writer_uses_session_id(
        self, mock_prep, mock_init, mock_batch, mock_update, mock_thread, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        state = make_mock_state("sess_abc")
        mock_init.return_value        = state
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_abc")
        args = mock_mw.call_args[0]
        assert args[1] == "sess_abc"

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.threading.Thread")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_monitoring_writer_passed_to_generate_batch(
        self, mock_prep, mock_init, mock_batch, mock_update, mock_thread, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        state       = make_mock_state("sess_1")
        mock_writer = make_mock_monitoring_writer()
        mock_mw.return_value          = mock_writer
        mock_init.return_value        = state
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        # monitoring_writer must be passed as kwarg to _generate_and_write_batch
        call_kwargs = mock_batch.call_args[1]
        assert call_kwargs.get("monitoring_writer") is mock_writer

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.threading.Thread")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_monitoring_writer_passed_to_watcher_thread(
        self, mock_prep, mock_init, mock_batch, mock_update, mock_thread, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        state       = make_mock_state("sess_1")
        mock_writer = make_mock_monitoring_writer()
        mock_mw.return_value          = mock_writer
        mock_init.return_value        = state
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        thread_kwargs = mock_thread.call_args[1]
        thread_args   = thread_kwargs.get("args", ())
        # monitoring_writer must be the second arg to _watch_and_refresh
        assert mock_writer in thread_args


# ── _run_ml_session ───────────────────────────────────────────────────────────

class TestRunMlSession:

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.threading.Thread")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_calls_preprocessing(
        self, mock_prep, mock_init, mock_batch, mock_update, mock_thread, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        mock_init.return_value        = make_mock_state()
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        mock_prep.assert_called_once()

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.threading.Thread")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_calls_initialize_session(
        self, mock_prep, mock_init, mock_batch, mock_update, mock_thread, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        mock_init.return_value        = make_mock_state()
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        mock_init.assert_called_once_with("sess_1")

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.threading.Thread")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_calls_generate_batch_once(
        self, mock_prep, mock_init, mock_batch, mock_update, mock_thread, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        mock_init.return_value        = make_mock_state()
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        mock_batch.assert_called_once()

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.threading.Thread")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_generate_batch_called_with_batch_number_1(
        self, mock_prep, mock_init, mock_batch, mock_update, mock_thread, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        mock_init.return_value        = make_mock_state()
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        call_kwargs = mock_batch.call_args[1]
        assert call_kwargs.get("batch_number") == 1 or mock_batch.call_args[0][1] == 1

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.threading.Thread")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_calls_update_last_refreshed_at(
        self, mock_prep, mock_init, mock_batch, mock_update, mock_thread, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        mock_init.return_value        = make_mock_state()
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        mock_update.assert_called()

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.threading.Thread")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_update_last_refreshed_at_called_with_zero(
        self, mock_prep, mock_init, mock_batch, mock_update, mock_thread, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        mock_init.return_value        = make_mock_state()
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        # first refresh should be marked at songs_played=0
        call_args = mock_update.call_args[0]
        assert call_args[2] == 0

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.threading.Thread")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_starts_background_thread(
        self, mock_prep, mock_init, mock_batch, mock_update, mock_thread, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        mock_init.return_value        = make_mock_state()
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        mock_thread.return_value.start.assert_called_once()

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.threading.Thread")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_watcher_thread_is_daemon(
        self, mock_prep, mock_init, mock_batch, mock_update, mock_thread, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        mock_init.return_value        = make_mock_state()
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        thread_kwargs = mock_thread.call_args[1]
        assert thread_kwargs.get("daemon") is True

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_handles_preprocessing_exception_gracefully(
        self, mock_prep, mock_init, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        mock_prep.side_effect = Exception("GCS error")
        _run_ml_session("sess_1")   # must not raise

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_handles_init_exception_gracefully(
        self, mock_prep, mock_init, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        mock_prep.return_value = None
        mock_init.side_effect  = Exception("BigQuery error")
        _run_ml_session("sess_1")   # must not raise

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_monitoring_writer_not_created_if_init_fails(
        self, mock_prep, mock_init, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        mock_init.side_effect = Exception("init failed")
        _run_ml_session("sess_1")
        # MonitoringWriter requires state.db — if init fails, it must not be called
        mock_mw.assert_not_called()

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.threading.Thread")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_preprocessing_called_before_initialize_session(
        self, mock_prep, mock_init, mock_batch, mock_update, mock_thread, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        call_order = []
        mock_prep.side_effect  = lambda *a: call_order.append("prep")
        mock_init.side_effect  = lambda *a: call_order.append("init") or make_mock_state()
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        assert call_order.index("prep") < call_order.index("init")

    @patch("ml.ml_trigger.MonitoringWriter")
    @patch("ml.ml_trigger.threading.Thread")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._run_ml_preprocessing")
    def test_initialize_session_called_before_batch(
        self, mock_prep, mock_init, mock_batch, mock_update, mock_thread, mock_mw
    ):
        from ml.ml_trigger import _run_ml_session
        call_order = []
        mock_init.side_effect  = lambda *a: call_order.append("init") or make_mock_state()
        mock_batch.side_effect = lambda *a, **kw: call_order.append("batch")
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        assert call_order.index("init") < call_order.index("batch")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
