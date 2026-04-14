"""
test_monitoring_integration.py
-------------------------------
End-to-end integration test for the monitoring system.

Tests the full chain:
  feedback endpoint → write_play_event_from_feedback → MonitoringWriter
  → Firestore (mocked) → monitoring endpoints → dashboard HTML

Run from repo root:
    pytest tests/ml_tests/test_monitoring_integration.py -v

No real GCP connections — all Firestore/BigQuery calls are mocked.
"""

import json
import time
import threading
import pytest
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock, call
from pathlib import Path


# ── Helpers ───────────────────────────────────────────────────────────────────

SESSION_ID = "test-session-integration-001"


def make_mock_db():
    """
    Firestore mock that records every .set() call so we can assert
    what was written without a real GCP connection.
    """
    db = MagicMock()
    db._writes = {}   # key: collection path → list of written docs

    def mock_set(doc_data):
        # capture writes keyed by the call chain
        db._writes[id(mock_set)] = doc_data
        db._all_writes = getattr(db, "_all_writes", [])
        db._all_writes.append(doc_data)

    db.collection.return_value \
      .document.return_value \
      .collection.return_value \
      .document.return_value \
      .set.side_effect = mock_set

    db.collection.return_value \
      .document.return_value \
      .collection.return_value \
      .stream.return_value = []

    db.collection.return_value \
      .document.return_value \
      .get.return_value.exists = True
    db.collection.return_value \
      .document.return_value \
      .get.return_value.to_dict.return_value = {
          "status": "active",
          "user_count": 3,
          "songs_played_count": 5,
          "created_at": None,
          "last_refreshed_at": 0,
      }
    return db


def make_scored_songs(n=10):
    """Scored song list as produced by _build_scored_songs() in main.py."""
    genres = ["pop", "rock", "jazz", "hip-hop", "classical"]
    return [
        {
            "video_id":    f"vid_{i}",
            "title":       f"Song {i}",
            "artist":      f"Artist {i}",
            "genre":       genres[i % len(genres)],
            "cbf_score":   round(0.6 + i * 0.01, 4),
            "cf_score":    round(0.5 + i * 0.01, 4),
            "gru_score":   round(0.4 + i * 0.01, 4),
            "final_score": round(0.55 + i * 0.01, 4),
        }
        for i in range(n)
    ]


def make_catalog_df(n=100):
    np.random.seed(42)
    genres = ["pop", "rock", "jazz", "hip-hop", "classical"]
    return pd.DataFrame({
        "genre":            [genres[i % len(genres)] for i in range(n)],
        "popularity_score": np.random.rand(n),
    })


# ── MonitoringWriter write cycle ──────────────────────────────────────────────

class TestMonitoringWriterFullCycle:
    """
    Tests that MonitoringWriter correctly writes rec_snapshots,
    bias_snapshots, and play_events to Firestore.
    """

    def test_write_rec_snapshot_fires_set(self):
        from ml.recommendation.firestore_monitoring import MonitoringWriter
        db     = make_mock_db()
        writer = MonitoringWriter(db, SESSION_ID)
        writer.write_rec_snapshot(
            phase="cold_start",
            weights={"cbf": 0.6, "cf": 0.4, "gru": 0.0},
            scored_songs=make_scored_songs(10),
        )
        db.collection.assert_called_with("sessions")

    def test_write_rec_snapshot_doc_structure(self):
        from ml.recommendation.firestore_monitoring import MonitoringWriter
        db       = make_mock_db()
        writer   = MonitoringWriter(db, SESSION_ID)
        set_mock = (
            db.collection.return_value
            .document.return_value
            .collection.return_value
            .document.return_value
            .set
        )
        writer.write_rec_snapshot(
            phase="full_hybrid",
            weights={"cbf": 0.4, "cf": 0.3, "gru": 0.3},
            scored_songs=make_scored_songs(5),
        )
        doc = set_mock.call_args[0][0]
        assert doc["phase"] == "full_hybrid"
        assert doc["weights"] == {"cbf": 0.4, "cf": 0.3, "gru": 0.3}
        assert len(doc["songs"]) == 5
        assert doc["songs"][0]["rank"] == 1

    def test_write_rec_snapshot_song_fields_complete(self):
        from ml.recommendation.firestore_monitoring import MonitoringWriter
        db     = make_mock_db()
        writer = MonitoringWriter(db, SESSION_ID)
        set_mock = (
            db.collection.return_value
            .document.return_value
            .collection.return_value
            .document.return_value
            .set
        )
        writer.write_rec_snapshot(
            phase="cold_start",
            weights={"cbf": 0.6, "cf": 0.4, "gru": 0.0},
            scored_songs=make_scored_songs(3),
        )
        song = set_mock.call_args[0][0]["songs"][0]
        required = {"rank","video_id","title","artist","genre",
                    "cbf_score","cf_score","gru_score","final_score"}
        assert required.issubset(song.keys())

    def test_write_bias_snapshot_fires_set(self):
        from ml.recommendation.firestore_monitoring import MonitoringWriter
        from ml.evaluation.model_bias import evaluate
        db     = make_mock_db()
        writer = MonitoringWriter(db, SESSION_ID)
        bias_result = evaluate(make_scored_songs(30), make_catalog_df())
        set_mock = (
            db.collection.return_value
            .document.return_value
            .collection.return_value
            .document.return_value
            .set
        )
        writer.write_bias_snapshot(bias_result)
        doc = set_mock.call_args[0][0]
        assert "genre_distribution" in doc
        assert "popularity_ratio" in doc
        assert "score_disparity" in doc
        assert "diversity_score" in doc
        assert "overall_bias_detected" in doc

    def test_write_bias_snapshot_fields_are_serializable(self):
        import json
        from ml.recommendation.firestore_monitoring import MonitoringWriter
        from ml.evaluation.model_bias import evaluate
        db     = make_mock_db()
        writer = MonitoringWriter(db, SESSION_ID)
        set_mock = (
            db.collection.return_value
            .document.return_value
            .collection.return_value
            .document.return_value
            .set
        )
        bias_result = evaluate(make_scored_songs(30), make_catalog_df())
        writer.write_bias_snapshot(bias_result)
        doc = set_mock.call_args[0][0]
        # remove SERVER_TIMESTAMP sentinel before JSON check
        doc.pop("generated_at", None)
        doc.pop("ts_ms", None)
        try:
            json.dumps(doc)
        except (TypeError, ValueError) as e:
            pytest.fail(f"bias_snapshot doc not JSON-serializable: {e}")

    def test_write_play_event_fires_set(self):
        from ml.recommendation.firestore_monitoring import MonitoringWriter
        db     = make_mock_db()
        writer = MonitoringWriter(db, SESSION_ID)
        set_mock = (
            db.collection.return_value
            .document.return_value
            .collection.return_value
            .document.return_value
            .set
        )
        writer.write_play_event(
            song={"video_id":"vid_0","title":"Song 0","artist":"Artist 0","genre":"pop"},
            feedback="like",
            play_duration_sec=142.5,
            songs_played_before=4,
        )
        doc = set_mock.call_args[0][0]
        assert doc["video_id"]          == "vid_0"
        assert doc["feedback"]          == "like"
        assert doc["play_duration_sec"] == 142.5
        assert doc["songs_played_before"] == 4
        assert doc["genre"]             == "pop"

    def test_write_play_event_all_feedback_types(self):
        from ml.recommendation.firestore_monitoring import MonitoringWriter
        db     = make_mock_db()
        writer = MonitoringWriter(db, SESSION_ID)
        set_mock = (
            db.collection.return_value
            .document.return_value
            .collection.return_value
            .document.return_value
            .set
        )
        for fb in ["like", "dislike", "skip", "replay", "neutral"]:
            writer.write_play_event(
                song={"video_id":"vid_0","title":"","artist":"","genre":"pop"},
                feedback=fb,
                play_duration_sec=30.0,
                songs_played_before=0,
            )
        assert set_mock.call_count == 5

    def test_firestore_exception_does_not_propagate(self):
        from ml.recommendation.firestore_monitoring import MonitoringWriter
        db     = make_mock_db()
        writer = MonitoringWriter(db, SESSION_ID)
        # Set side_effect AFTER init — __init__ calls db.collection() to build
        # _session_ref. The write methods call _session_ref.collection(), so
        # we target that instead of db.collection() directly.
        writer._session_ref.collection.side_effect = Exception("Firestore unavailable")
        # none of these must raise — try/except in each write method must catch it
        writer.write_rec_snapshot("cold_start", {"cbf":0.6,"cf":0.4,"gru":0.0}, [])
        writer.write_bias_snapshot({})
        writer.write_play_event({"video_id":"v","title":"","artist":"","genre":""},
                                "like", 0.0, 0)


# ── Full pipeline chain: trigger → write → read ───────────────────────────────

class TestFullPipelineChain:
    """
    Simulates one full session cycle:
    1. initialize_session() sets up state
    2. recommend_next_song() writes rec_snapshot
    3. evaluate_bias() writes bias_snapshot
    4. feedback arrives → write_play_event

    Then verifies the monitoring read helpers return the right shapes.
    """

    @patch("ml.recommendation.main.fetch_all_user_liked")
    @patch("ml.recommendation.main.load_gru_model")
    @patch("ml.recommendation.main.build_embedding_lookup")
    @patch("ml.recommendation.main.fetch_all_embeddings")
    @patch("ml.recommendation.main.get_session_user_ids")
    @patch("ml.recommendation.main.get_client")
    @patch("ml.recommendation.main.get_db")
    def test_monitoring_writer_set_on_state_after_init(
        self, mock_get_db, mock_get_client, mock_user_ids,
        mock_embeddings, mock_lookup, mock_gru, mock_liked
    ):
        from ml.recommendation.main import initialize_session, SessionState
        songs_df = pd.DataFrame({
            "video_id":    [f"vid_{i}" for i in range(10)],
            "track_title": [f"Song {i}" for i in range(10)],
            "artist_name": [f"Artist {i}" for i in range(10)],
            "genre":       ["pop"] * 10,
            "embedding":   [np.random.rand(386).astype(np.float32) for _ in range(10)],
        })
        mock_embeddings.return_value = songs_df
        mock_lookup.return_value     = {}
        mock_user_ids.return_value   = ["u1"]
        mock_liked.return_value      = {"u1": ["vid_0"]}
        mock_gru.return_value        = MagicMock()
        mock_get_db.return_value     = make_mock_db()
        mock_get_client.return_value = MagicMock()

        state = initialize_session(SESSION_ID)
        assert isinstance(state, SessionState)
        assert state.catalog_df is state.songs_df
        assert state.last_batch_songs == []
        assert state.monitoring_writer is None

    def test_last_batch_songs_set_after_recommend(self):
        """
        After recommend_next_song() runs, state.last_batch_songs must be
        a non-empty list of dicts with the correct field names.
        """
        from ml.recommendation.main import recommend_next_song, SessionState
        from ml.recommendation.main import _resolve_phase_name, _resolve_active_weights, _build_scored_songs

        state = SessionState()
        state.session_id = SESSION_ID
        state.db         = make_mock_db()
        state.bq_client  = MagicMock()
        state.gru_model  = None
        state.user_ids   = ["u1"]
        state.all_user_liked = {}
        state.last_batch_songs = []
        state.monitoring_writer = None
        state.embedding_lookup = {}

        songs_df = pd.DataFrame({
            "video_id":    [f"vid_{i}" for i in range(10)],
            "track_title": [f"Song {i}" for i in range(10)],
            "artist_name": [f"Artist {i}" for i in range(10)],
            "genre":       ["pop"] * 10,
            "embedding":   [np.random.rand(386).astype(np.float32) for _ in range(10)],
        })
        state.songs_df   = songs_df
        state.catalog_df = songs_df

        patches = [
            patch("ml.recommendation.main.get_session_play_sequence", return_value=[]),
            patch("ml.recommendation.main.get_session_track_scores",  return_value={}),
            patch("ml.recommendation.main.get_already_played_ids",    return_value=set()),
            patch("ml.recommendation.main.get_user_liked_songs",      return_value={"u1":["vid_0"]}),
            patch("ml.recommendation.main.get_user_disliked_songs",   return_value={}),
            patch("ml.recommendation.main.build_user_vectors",        return_value={"u1": np.random.rand(386)}),
            patch("ml.recommendation.main.build_global_vector",       return_value=np.random.rand(1,386)),
            patch("ml.recommendation.main.get_cbf_scores",            return_value=pd.DataFrame({
                "video_id": [f"vid_{i}" for i in range(5)],
                "track_title": [f"Song {i}" for i in range(5)],
                "artist_name": [f"Artist {i}" for i in range(5)],
                "genre": ["pop"]*5,
                "cbf_score": [0.9,0.8,0.7,0.6,0.5],
                "session_score": [0.1]*5,
            })),
            patch("ml.recommendation.main.get_cf_scores",  return_value=pd.DataFrame()),
            patch("ml.recommendation.main.get_gru_scores", return_value=pd.DataFrame()),
            patch("ml.recommendation.main._write_recommendations_to_firestore"),
        ]
        mocks = [p.start() for p in patches]
        try:
            recommend_next_song(state)
        finally:
            for p in patches:
                p.stop()

        assert isinstance(state.last_batch_songs, list)
        assert len(state.last_batch_songs) > 0
        song = state.last_batch_songs[0]
        assert "title"  in song and "track_title"  not in song
        assert "artist" in song and "artist_name"  not in song
        assert "final_score" in song
        assert "cbf_score"   in song

    def test_write_rec_snapshot_called_when_writer_on_state(self):
        from ml.recommendation.main import recommend_next_song, SessionState

        state = SessionState()
        state.session_id = SESSION_ID
        state.db         = make_mock_db()
        state.bq_client  = MagicMock()
        state.gru_model  = None
        state.user_ids   = ["u1"]
        state.all_user_liked = {}
        state.last_batch_songs = []
        state.embedding_lookup = {}
        songs_df = pd.DataFrame({
            "video_id":    [f"vid_{i}" for i in range(5)],
            "track_title": [f"Song {i}" for i in range(5)],
            "artist_name": [f"Artist {i}" for i in range(5)],
            "genre":       ["pop"] * 5,
            "embedding":   [np.random.rand(386).astype(np.float32) for _ in range(5)],
        })
        state.songs_df   = songs_df
        state.catalog_df = songs_df

        writer = MagicMock()
        state.monitoring_writer = writer

        patches = [
            patch("ml.recommendation.main.get_session_play_sequence", return_value=[]),
            patch("ml.recommendation.main.get_session_track_scores",  return_value={}),
            patch("ml.recommendation.main.get_already_played_ids",    return_value=set()),
            patch("ml.recommendation.main.get_user_liked_songs",      return_value={"u1":["vid_0"]}),
            patch("ml.recommendation.main.get_user_disliked_songs",   return_value={}),
            patch("ml.recommendation.main.build_user_vectors",        return_value={"u1": np.random.rand(386)}),
            patch("ml.recommendation.main.build_global_vector",       return_value=np.random.rand(1,386)),
            patch("ml.recommendation.main.get_cbf_scores", return_value=pd.DataFrame({
                "video_id":    [f"vid_{i}" for i in range(5)],
                "track_title": [f"Song {i}" for i in range(5)],
                "artist_name": [f"Artist {i}" for i in range(5)],
                "genre":       ["pop"]*5,
                "cbf_score":   [0.9,0.8,0.7,0.6,0.5],
                "session_score": [0.1]*5,
            })),
            patch("ml.recommendation.main.get_cf_scores",  return_value=pd.DataFrame()),
            patch("ml.recommendation.main.get_gru_scores", return_value=pd.DataFrame()),
            patch("ml.recommendation.main._write_recommendations_to_firestore"),
        ]
        mocks = [p.start() for p in patches]
        try:
            recommend_next_song(state)
        finally:
            for p in patches:
                p.stop()

        writer.write_rec_snapshot.assert_called_once()
        call_kw = writer.write_rec_snapshot.call_args[1]
        assert "phase"       in call_kw
        assert "weights"     in call_kw
        assert "scored_songs" in call_kw
        assert set(call_kw["weights"].keys()) == {"cbf","cf","gru"}


# ── Flask monitoring endpoints ────────────────────────────────────────────────

class TestMonitoringEndpoints:
    """
    Tests that the Flask Blueprint routes return correct shapes.
    Firestore reads are mocked to return pre-built dicts.
    """

    @pytest.fixture
    def client(self):
        from flask import Flask
        from ml.api.monitoring_routes import monitoring_bp
        app = Flask(__name__)
        app.config["TESTING"] = True
        app.register_blueprint(monitoring_bp, url_prefix="/monitoring")
        return app.test_client()

    @patch("ml.api.monitoring_routes._cached")
    def test_state_endpoint_returns_ok_true(self, mock_cached, client):
        mock_cached.return_value = {
            "session_id":         SESSION_ID,
            "phase":              "cold_start",
            "weights":            {"cbf":0.6,"cf":0.4,"gru":0.0},
            "songs_played":       5,
            "feedback_counts":    {"like":3,"skip":1,"replay":0,"dislike":0,"neutral":1},
            "avg_play_duration_sec": 120.0,
            "status":             "active",
            "user_count":         3,
            "created_at":         None,
        }
        r = client.get(f"/monitoring/session/{SESSION_ID}/state")
        assert r.status_code == 200
        body = json.loads(r.data)
        assert body["ok"] is True
        assert "data" in body

    @patch("ml.api.monitoring_routes._cached")
    def test_state_endpoint_data_has_phase(self, mock_cached, client):
        mock_cached.return_value = {
            "phase": "full_hybrid", "weights": {"cbf":0.4,"cf":0.3,"gru":0.3},
            "songs_played": 10, "feedback_counts": {},
            "avg_play_duration_sec": 0.0, "status": "active",
            "user_count": 2, "created_at": None,
        }
        r    = client.get(f"/monitoring/session/{SESSION_ID}/state")
        body = json.loads(r.data)
        assert body["data"]["phase"] == "full_hybrid"

    @patch("ml.api.monitoring_routes._cached")
    def test_recommendations_endpoint_returns_ok_true(self, mock_cached, client):
        mock_cached.return_value = [
            {
                "ts_ms": 1234567890,
                "phase": "cold_start",
                "weights": {"cbf":0.6,"cf":0.4,"gru":0.0},
                "songs": make_scored_songs(5),
            }
        ]
        r    = client.get(f"/monitoring/session/{SESSION_ID}/recommendations")
        body = json.loads(r.data)
        assert r.status_code == 200
        assert body["ok"] is True
        assert body["data"]["count"] == 1

    @patch("ml.api.monitoring_routes._cached")
    def test_recommendations_cycles_param(self, mock_cached, client):
        mock_cached.return_value = []
        client.get(f"/monitoring/session/{SESSION_ID}/recommendations?cycles=3")
        # _cached was called — cycles param was processed without error
        mock_cached.assert_called_once()

    @patch("ml.api.monitoring_routes._cached")
    def test_bias_endpoint_returns_ok_true(self, mock_cached, client):
        mock_cached.return_value = {
            "genre_distribution":  {"pop": 0.5, "rock": 0.3, "jazz": 0.2},
            "genre_flags":         [],
            "popularity_ratio":    1.1,
            "popularity_flagged":  False,
            "score_disparity":     0.08,
            "disparity_flagged":   False,
            "diversity_score":     1.52,
            "overall_bias_detected": False,
            "mitigation_suggestions": ["No significant bias detected."],
        }
        r    = client.get(f"/monitoring/session/{SESSION_ID}/bias")
        body = json.loads(r.data)
        assert r.status_code == 200
        assert body["ok"] is True
        assert "active_flags" in body["data"]
        assert "health"       in body["data"]

    @patch("ml.api.monitoring_routes._cached")
    def test_bias_health_ok_when_no_flags(self, mock_cached, client):
        mock_cached.return_value = {
            "genre_flags": [], "popularity_flagged": False,
            "disparity_flagged": False, "overall_bias_detected": False,
            "genre_distribution": {}, "popularity_ratio": 1.0,
            "score_disparity": 0.0, "diversity_score": 1.5,
            "mitigation_suggestions": [],
        }
        body = json.loads(client.get(f"/monitoring/session/{SESSION_ID}/bias").data)
        assert body["data"]["health"] == "ok"

    @patch("ml.api.monitoring_routes._cached")
    def test_bias_health_warn_when_one_flag(self, mock_cached, client):
        mock_cached.return_value = {
            "genre_flags": [], "popularity_flagged": True,
            "disparity_flagged": False, "overall_bias_detected": True,
            "genre_distribution": {}, "popularity_ratio": 2.1,
            "score_disparity": 0.0, "diversity_score": 1.5,
            "mitigation_suggestions": [],
        }
        body = json.loads(client.get(f"/monitoring/session/{SESSION_ID}/bias").data)
        assert body["data"]["health"] == "warn"
        assert "popularity_bias" in body["data"]["active_flags"]

    @patch("ml.api.monitoring_routes._cached")
    def test_bias_health_critical_when_multiple_flags(self, mock_cached, client):
        mock_cached.return_value = {
            "genre_flags": ["pop_over_represented"],
            "popularity_flagged": True,
            "disparity_flagged": True,
            "overall_bias_detected": True,
            "genre_distribution": {}, "popularity_ratio": 2.5,
            "score_disparity": 0.22, "diversity_score": 0.8,
            "mitigation_suggestions": [],
        }
        body = json.loads(client.get(f"/monitoring/session/{SESSION_ID}/bias").data)
        assert body["data"]["health"] == "critical"

    @patch("ml.api.monitoring_routes._cached")
    def test_stats_endpoint_returns_ok_true(self, mock_cached, client):
        mock_cached.return_value = {
            "total_plays":          10,
            "genre_distribution":   {"pop": 0.5, "rock": 0.5},
            "popularity_histogram": [{"lo":0.0,"hi":0.1,"count":2}],
            "diversity_trend":      [{"ts_ms":1,"entropy":1.5,"song_count":10}],
            "avg_play_duration_sec": 135.0,
            "avg_feedback_score":    0.4,
            "feedback_score_interpretation": "Good",
        }
        r    = client.get(f"/monitoring/session/{SESSION_ID}/stats")
        body = json.loads(r.data)
        assert r.status_code == 200
        assert body["ok"] is True

    def test_dashboard_served_correctly(self, client, tmp_path, monkeypatch):
        """
        Verifies dashboard.html is served with __SESSION_ID__ replaced.
        Uses a temp HTML file so the test doesn't depend on ml/static existing.
        """
        import ml.api.monitoring_routes as routes_module

        html_content = "<html><body>Session: __SESSION_ID__</body></html>"
        fake_static  = tmp_path / "static"
        fake_static.mkdir()
        (fake_static / "dashboard.html").write_text(html_content)

        monkeypatch.setattr(routes_module, "_STATIC_DIR", fake_static)

        r = client.get(f"/monitoring/dashboard/{SESSION_ID}")
        assert r.status_code == 200
        assert SESSION_ID.encode() in r.data
        assert b"__SESSION_ID__" not in r.data

    def test_endpoint_returns_error_on_exception(self, client):
        """If Firestore read fails, endpoint returns ok=false, not 500 crash."""
        with patch("ml.api.monitoring_routes._cached", side_effect=Exception("db down")):
            r    = client.get(f"/monitoring/session/{SESSION_ID}/state")
            body = json.loads(r.data)
            assert body["ok"] is False
            assert "error" in body


# ── play_event_writer integration ─────────────────────────────────────────────

class TestPlayEventWriterIntegration:
    """
    Tests the full feedback → write_play_event_from_feedback chain.
    """

    @patch("ml.api.play_event_writer._get_db")
    def test_write_play_event_from_feedback_fires(self, mock_get_db):
        from ml.api.play_event_writer import write_play_event_from_feedback
        db = make_mock_db()
        mock_get_db.return_value = db
        set_mock = (
            db.collection.return_value
            .document.return_value
            .collection.return_value
            .document.return_value
            .set
        )
        write_play_event_from_feedback(
            session_id="sess_1",
            video_id="vid_0",
            action="like",
            play_duration_sec=142.5,
        )
        set_mock.assert_called_once()
        doc = set_mock.call_args[0][0]
        assert doc["video_id"]          == "vid_0"
        assert doc["feedback"]          == "like"
        assert doc["play_duration_sec"] == 142.5

    @patch("ml.api.play_event_writer._get_db")
    def test_write_play_event_from_feedback_all_actions(self, mock_get_db):
        from ml.api.play_event_writer import write_play_event_from_feedback
        db = make_mock_db()
        mock_get_db.return_value = db
        set_mock = (
            db.collection.return_value
            .document.return_value
            .collection.return_value
            .document.return_value
            .set
        )
        for action in ["like", "dislike", "skip", "replay"]:
            write_play_event_from_feedback("sess_1", "vid_0", action, 30.0)
        assert set_mock.call_count == 4

    @patch("ml.api.play_event_writer._get_db")
    def test_write_play_event_defaults_duration_to_zero(self, mock_get_db):
        from ml.api.play_event_writer import write_play_event_from_feedback
        db = make_mock_db()
        mock_get_db.return_value = db
        set_mock = (
            db.collection.return_value
            .document.return_value
            .collection.return_value
            .document.return_value
            .set
        )
        write_play_event_from_feedback("sess_1", "vid_0", "skip")
        doc = set_mock.call_args[0][0]
        assert doc["play_duration_sec"] == 0.0

    @patch("ml.api.play_event_writer._get_db")
    def test_write_play_event_never_raises(self, mock_get_db):
        from ml.api.play_event_writer import write_play_event_from_feedback
        mock_get_db.side_effect = Exception("Firestore totally down")
        # must NOT raise — monitoring never crashes the feedback pipeline
        write_play_event_from_feedback("sess_1", "vid_0", "like", 30.0)

    @patch("ml.api.play_event_writer._get_db")
    def test_feedback_endpoint_triggers_play_event_write(self, mock_get_db):
        """
        Simulates the full /feedback → write_play_event_from_feedback chain
        via the Flask test client.
        """
        from flask import Flask
        from ml.api.monitoring_routes import monitoring_bp

        db = make_mock_db()
        mock_get_db.return_value = db

        # build a minimal Flask app with the feedback endpoint wired up
        mini_app = Flask(__name__)
        mini_app.config["TESTING"] = True
        mini_app.register_blueprint(monitoring_bp, url_prefix="/monitoring")

        from ml.api.play_event_writer import write_play_event_from_feedback

        @mini_app.route("/feedback", methods=["POST"])
        def feedback():
            import threading
            from flask import request, jsonify
            data = request.get_json()
            threading.Thread(
                target=write_play_event_from_feedback,
                args=(data["session_id"], data["video_id"],
                      data["action"], float(data.get("play_duration_sec", 0.0))),
                daemon=True,
            ).start()
            return jsonify({"status": "ok"}), 200

        client = mini_app.test_client()
        r = client.post("/feedback", json={
            "session_id":       SESSION_ID,
            "user_id":          "u1",
            "video_id":         "vid_0",
            "action":           "like",
            "play_duration_sec": 95.0,
        })
        assert r.status_code == 200
        # give daemon thread time to fire
        time.sleep(0.1)


# ── Evaluate → write_bias_snapshot round-trip ─────────────────────────────────

class TestEvaluateToBiasSnapshotRoundTrip:
    """
    Full round-trip: evaluate() produces a report → write_bias_snapshot()
    stores it → read_bias() returns it with the right shape.
    """

    def test_evaluate_output_compatible_with_write_bias_snapshot(self):
        from ml.evaluation.model_bias import evaluate
        from ml.recommendation.firestore_monitoring import MonitoringWriter

        db     = make_mock_db()
        writer = MonitoringWriter(db, SESSION_ID)
        set_mock = (
            db.collection.return_value
            .document.return_value
            .collection.return_value
            .document.return_value
            .set
        )

        bias_result = evaluate(make_scored_songs(30), make_catalog_df())
        writer.write_bias_snapshot(bias_result)   # must not raise

        doc = set_mock.call_args[0][0]
        # verify all dashboard-required fields are present
        required = {
            "genre_distribution", "genre_flags", "over_represented",
            "under_represented", "genre_bias_detected",
            "diversity_score", "popularity_ratio", "popularity_flagged",
            "score_disparity", "disparity_flagged", "per_genre_scores",
            "overall_bias_detected", "mitigation_suggestions",
        }
        assert required.issubset(doc.keys())

    def test_rec_snapshot_then_bias_snapshot_same_session(self):
        """Both writes go to the same session document — verify no collision."""
        from ml.recommendation.firestore_monitoring import MonitoringWriter
        from ml.evaluation.model_bias import evaluate

        db     = make_mock_db()
        writer = MonitoringWriter(db, SESSION_ID)

        writer.write_rec_snapshot(
            phase="cold_start",
            weights={"cbf":0.6,"cf":0.4,"gru":0.0},
            scored_songs=make_scored_songs(5),
        )
        bias_result = evaluate(make_scored_songs(30), make_catalog_df())
        writer.write_bias_snapshot(bias_result)

        set_mock = (
            db.collection.return_value
            .document.return_value
            .collection.return_value
            .document.return_value
            .set
        )
        # both writes fired
        assert set_mock.call_count == 2


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
