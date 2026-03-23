"""
Tests for ml/ml_trigger.py

Covers:
  - _get_last_refreshed_at()
  - _run_ml_preprocessing()
  - _generate_and_write_batch()
  - _watch_and_refresh() (mocked)
  - _run_ml_session() (mocked)
  - ml_session_trigger() Cloud Function entry point
"""

import os
import pytest
from unittest.mock import patch, MagicMock, call


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


def make_mock_state(session_id="sess_1"):
    state            = MagicMock()
    state.session_id = session_id
    state.db         = make_mock_db()
    return state


# ── _get_last_refreshed_at tests ──────────────────────────────────────────────

class TestGetLastRefreshedAt:

    def _call(self, db, session_id="sess_1"):
        from ml.ml_trigger import _get_last_refreshed_at
        return _get_last_refreshed_at(db, session_id)

    def test_returns_int(self):
        db = make_mock_db(last_refreshed=5)
        assert isinstance(self._call(db), int)

    def test_returns_correct_value(self):
        db = make_mock_db(last_refreshed=10)
        assert self._call(db) == 10

    def test_returns_zero_when_doc_not_exists(self):
        db = make_mock_db(exists=False)
        assert self._call(db) == 0

    def test_returns_zero_on_exception(self):
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
        db = make_mock_db(last_refreshed=0)
        assert self._call(db) == 0

    def test_handles_large_value(self):
        db = make_mock_db(last_refreshed=999)
        assert self._call(db) == 999


# ── _run_ml_preprocessing tests ───────────────────────────────────────────────

class TestRunMlPreprocessing:

    @patch("ml.ml_trigger.preprocess_for_session")
    @patch("ml.ml_trigger.generate_embeddings_for_session")
    def test_calls_preprocess_for_session(self, mock_embed, mock_preprocess):
        from ml.ml_trigger import _run_ml_preprocessing
        mock_preprocess.return_value = "gs://bucket/preprocessed.csv"
        mock_embed.return_value      = 5
        _run_ml_preprocessing("sess_1", "my-bucket")
        mock_preprocess.assert_called_once_with("sess_1", "my-bucket")

    @patch("ml.ml_trigger.preprocess_for_session")
    @patch("ml.ml_trigger.generate_embeddings_for_session")
    def test_calls_generate_embeddings_for_session(self, mock_embed, mock_preprocess):
        from ml.ml_trigger import _run_ml_preprocessing
        mock_preprocess.return_value = "gs://bucket/preprocessed.csv"
        mock_embed.return_value      = 5
        _run_ml_preprocessing("sess_1", "my-bucket")
        mock_embed.assert_called_once_with("sess_1", "my-bucket")

    @patch("ml.ml_trigger.preprocess_for_session")
    @patch("ml.ml_trigger.generate_embeddings_for_session")
    def test_passes_correct_session_id(self, mock_embed, mock_preprocess):
        from ml.ml_trigger import _run_ml_preprocessing
        mock_preprocess.return_value = "gs://bucket/preprocessed.csv"
        mock_embed.return_value      = 0
        _run_ml_preprocessing("my-session-abc", "bucket")
        assert mock_preprocess.call_args[0][0] == "my-session-abc"
        assert mock_embed.call_args[0][0]      == "my-session-abc"

    @patch("ml.ml_trigger.preprocess_for_session")
    @patch("ml.ml_trigger.generate_embeddings_for_session")
    def test_passes_correct_bucket(self, mock_embed, mock_preprocess):
        from ml.ml_trigger import _run_ml_preprocessing
        mock_preprocess.return_value = "gs://bucket/preprocessed.csv"
        mock_embed.return_value      = 0
        _run_ml_preprocessing("sess_1", "my-special-bucket")
        assert mock_preprocess.call_args[0][1] == "my-special-bucket"
        assert mock_embed.call_args[0][1]      == "my-special-bucket"

    @patch("ml.ml_trigger.preprocess_for_session")
    @patch("ml.ml_trigger.generate_embeddings_for_session")
    def test_preprocess_called_before_embed(self, mock_embed, mock_preprocess):
        from ml.ml_trigger import _run_ml_preprocessing
        call_order = []
        mock_preprocess.side_effect = lambda *a: call_order.append("preprocess") or "path"
        mock_embed.side_effect      = lambda *a: call_order.append("embed") or 0
        _run_ml_preprocessing("sess_1", "bucket")
        assert call_order == ["preprocess", "embed"]

    @patch("ml.ml_trigger.preprocess_for_session")
    @patch("ml.ml_trigger.generate_embeddings_for_session")
    def test_zero_new_songs_no_error(self, mock_embed, mock_preprocess):
        from ml.ml_trigger import _run_ml_preprocessing
        mock_preprocess.return_value = "gs://bucket/preprocessed.csv"
        mock_embed.return_value      = 0
        _run_ml_preprocessing("sess_1", "bucket")
        mock_embed.assert_called_once()


# ── _generate_and_write_batch tests ──────────────────────────────────────────

class TestGenerateAndWriteBatch:

    @patch("ml.ml_trigger.recommend_next_song")
    def test_calls_recommend_next_song(self, mock_rec):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value = {"track_title": "Song A", "video_id": "vid_0"}
        state = make_mock_state()
        _generate_and_write_batch(state, batch_number=1)
        mock_rec.assert_called_once_with(state)

    @patch("ml.ml_trigger.recommend_next_song")
    def test_handles_empty_recommendations(self, mock_rec):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value = {}
        state = make_mock_state()
        # should not raise even when no recommendations returned
        _generate_and_write_batch(state, batch_number=1)
        mock_rec.assert_called_once()

    @patch("ml.ml_trigger.recommend_next_song")
    def test_handles_none_recommendations(self, mock_rec):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value = None
        state = make_mock_state()
        _generate_and_write_batch(state, batch_number=1)
        mock_rec.assert_called_once()

    @patch("ml.ml_trigger.recommend_next_song")
    def test_works_for_batch_2(self, mock_rec):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value = {"track_title": "Song B", "video_id": "vid_1"}
        state = make_mock_state()
        _generate_and_write_batch(state, batch_number=2)
        mock_rec.assert_called_once_with(state)

    @patch("ml.ml_trigger.recommend_next_song")
    def test_passes_state_to_recommend(self, mock_rec):
        from ml.ml_trigger import _generate_and_write_batch
        mock_rec.return_value = {"track_title": "Song", "video_id": "vid_0"}
        state = make_mock_state("test-session")
        _generate_and_write_batch(state, batch_number=1)
        called_state = mock_rec.call_args[0][0]
        assert called_state.session_id == "test-session"


# ── _run_ml_session tests ─────────────────────────────────────────────────────

class TestRunMlSession:

    @patch("ml.ml_trigger._run_ml_preprocessing")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger.threading.Thread")
    def test_calls_preprocessing(
        self, mock_thread, mock_update,
        mock_batch, mock_init, mock_preprocess
    ):
        from ml.ml_trigger import _run_ml_session
        mock_init.return_value        = make_mock_state()
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        mock_preprocess.assert_called_once()

    @patch("ml.ml_trigger._run_ml_preprocessing")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger.threading.Thread")
    def test_calls_initialize_session(
        self, mock_thread, mock_update,
        mock_batch, mock_init, mock_preprocess
    ):
        from ml.ml_trigger import _run_ml_session
        mock_init.return_value        = make_mock_state()
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        mock_init.assert_called_once_with("sess_1")

    @patch("ml.ml_trigger._run_ml_preprocessing")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger.threading.Thread")
    def test_calls_generate_batch(
        self, mock_thread, mock_update,
        mock_batch, mock_init, mock_preprocess
    ):
        from ml.ml_trigger import _run_ml_session
        mock_init.return_value        = make_mock_state()
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        mock_batch.assert_called_once()

    @patch("ml.ml_trigger._run_ml_preprocessing")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger.threading.Thread")
    def test_calls_update_last_refreshed_at(
        self, mock_thread, mock_update,
        mock_batch, mock_init, mock_preprocess
    ):
        from ml.ml_trigger import _run_ml_session
        mock_init.return_value        = make_mock_state()
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        mock_update.assert_called()

    @patch("ml.ml_trigger._run_ml_preprocessing")
    @patch("ml.ml_trigger.initialize_session")
    @patch("ml.ml_trigger._generate_and_write_batch")
    @patch("ml.ml_trigger.update_last_refreshed_at")
    @patch("ml.ml_trigger.threading.Thread")
    def test_starts_background_thread(
        self, mock_thread, mock_update,
        mock_batch, mock_init, mock_preprocess
    ):
        from ml.ml_trigger import _run_ml_session
        mock_init.return_value        = make_mock_state()
        mock_thread.return_value.join = MagicMock()
        _run_ml_session("sess_1")
        mock_thread.return_value.start.assert_called_once()

    @patch("ml.ml_trigger._run_ml_preprocessing")
    @patch("ml.ml_trigger.initialize_session")
    def test_handles_exception_gracefully(self, mock_init, mock_preprocess):
        from ml.ml_trigger import _run_ml_session
        mock_preprocess.side_effect = Exception("GCS error")
        # should not raise
        _run_ml_session("sess_1")

    @patch("ml.ml_trigger._run_ml_preprocessing")
    @patch("ml.ml_trigger.initialize_session")
    def test_handles_init_exception_gracefully(self, mock_init, mock_preprocess):
        from ml.ml_trigger import _run_ml_session
        mock_preprocess.return_value = None
        mock_init.side_effect        = Exception("BigQuery error")
        # should not raise
        _run_ml_session("sess_1")


# ── ml_session_trigger Cloud Function tests ───────────────────────────────────

class TestMlSessionTrigger:

    def _make_cloud_event(self, session_id="sess_abc"):
        import base64
        import json
        payload = json.dumps({"session_id": session_id}).encode("utf-8")
        data    = base64.b64encode(payload).decode("utf-8")
        event   = MagicMock()
        event.data = {"message": {"data": data}}
        return event

    @patch("ml.ml_trigger._run_ml_session")
    @patch("ml.ml_trigger.threading.Thread")
    def test_starts_thread(self, mock_thread, mock_run):
        from ml.ml_trigger import ml_session_trigger
        event = self._make_cloud_event("sess_abc")
        ml_session_trigger(event)
        mock_thread.return_value.start.assert_called_once()

    @patch("ml.ml_trigger._run_ml_session")
    @patch("ml.ml_trigger.threading.Thread")
    def test_decodes_session_id(self, mock_thread, mock_run):
        from ml.ml_trigger import ml_session_trigger
        event = self._make_cloud_event("my-session-123")
        ml_session_trigger(event)
        thread_kwargs = mock_thread.call_args[1]
        assert "my-session-123" in thread_kwargs.get("args", ())

    @patch("ml.ml_trigger._run_ml_session")
    @patch("ml.ml_trigger.threading.Thread")
    def test_thread_is_daemon(self, mock_thread, mock_run):
        from ml.ml_trigger import ml_session_trigger
        event = self._make_cloud_event()
        ml_session_trigger(event)
        assert mock_thread.call_args[1].get("daemon") is True

    def test_handles_invalid_payload_gracefully(self):
        from ml.ml_trigger import ml_session_trigger
        event      = MagicMock()
        event.data = {"message": {"data": "not_valid_base64!!!"}}
        # should not raise
        ml_session_trigger(event)

    def test_handles_missing_session_id_gracefully(self):
        import base64
        import json
        from ml.ml_trigger import ml_session_trigger
        payload    = json.dumps({"wrong_key": "value"}).encode("utf-8")
        data       = base64.b64encode(payload).decode("utf-8")
        event      = MagicMock()
        event.data = {"message": {"data": data}}
        # should not raise
        ml_session_trigger(event)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])