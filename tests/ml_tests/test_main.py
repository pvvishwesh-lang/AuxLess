"""
Tests for ml/recommendation/main.py

Covers:
  - SessionState.__init__()
  - _write_recommendations_to_firestore()
  - _resolve_gru_weight()
  - _aggregate_scores()
  - _resolve_phase_name()          ← new monitoring helper
  - _resolve_active_weights()      ← new monitoring helper
  - _build_scored_songs()          ← new monitoring helper
  - initialize_session()
  - recommend_next_song()          ← including monitoring side effects

All GCP calls (BigQuery, Firestore, GCS) are mocked.
No real connections made.
"""

import pytest
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock, call


# ── Config values mirrored from main.py / config.py ──────────────────────────
# Kept here so tests are self-documenting. Must stay in sync with config.py.
SESSION_SCORE_WEIGHT   = 0.3
GRU_ACTIVATION_THRESHOLD = 3   # GRU activates after 3 songs played


# ── Shared helpers ────────────────────────────────────────────────────────────

def make_songs_df(n=10, seed=42):
    """Minimal songs DataFrame matching fetch_all_embeddings() output."""
    np.random.seed(seed)
    genres = ["pop", "rock", "jazz", "hip-hop", "classical"]
    return pd.DataFrame({
        "video_id":    [f"vid_{i}"      for i in range(n)],
        "track_title": [f"Song {i}"     for i in range(n)],
        "artist_name": [f"Artist {i}"   for i in range(n)],
        "genre":       [genres[i % len(genres)] for i in range(n)],
        "embedding":   [np.random.rand(386).astype(np.float32) for _ in range(n)],
    })


def make_recommendations_df(n=5, with_gru=False, seed=42):
    """
    Minimal recommendations DataFrame as produced by _aggregate_scores().
    Matches the columns _write_recommendations_to_firestore() and
    _build_scored_songs() expect.
    """
    np.random.seed(seed)
    genres = ["pop", "rock", "jazz"]
    df = pd.DataFrame({
        "video_id":      [f"vid_{i}"    for i in range(n)],
        "track_title":   [f"Song {i}"  for i in range(n)],
        "artist_name":   [f"Artist {i}"for i in range(n)],
        "genre":         [genres[i % len(genres)] for i in range(n)],
        "cbf_score":     np.random.rand(n),
        "cf_score":      np.random.rand(n),
        "session_score": np.random.rand(n) * 0.5,
        "gru_score":     np.random.rand(n) if with_gru else [0.0] * n,
        "final_score":   np.random.rand(n),
    })
    return df.sort_values("final_score", ascending=False).reset_index(drop=True)


def make_mock_db():
    db = MagicMock()
    # stream() returns an iterable of docs (empty by default)
    db.collection.return_value \
      .document.return_value \
      .collection.return_value \
      .stream.return_value = []
    return db


def make_mock_state(session_id="sess_1", songs_played=0, gru_loaded=True):
    """
    Builds a mock SessionState with all required attributes.
    Does NOT use MagicMock for the state itself so attribute checks work
    correctly (e.g. state.monitoring_writer is None by default).
    """
    from ml.recommendation.main import SessionState
    state                  = SessionState()
    state.session_id       = session_id
    state.db               = make_mock_db()
    state.bq_client        = MagicMock()
    state.songs_df         = make_songs_df()
    state.catalog_df       = state.songs_df
    state.embedding_lookup = {f"vid_{i}": np.random.rand(386) for i in range(10)}
    state.gru_model        = MagicMock() if gru_loaded else None
    state.user_ids         = ["user_1", "user_2"]
    state.all_user_liked   = {"user_1": ["vid_0", "vid_1"], "user_2": ["vid_2"]}
    state.last_batch_songs = []
    state.monitoring_writer = None
    return state


def make_mock_monitoring_writer():
    writer = MagicMock()
    writer.write_rec_snapshot  = MagicMock()
    writer.write_bias_snapshot = MagicMock()
    return writer


# ── SessionState.__init__ ─────────────────────────────────────────────────────

class TestSessionState:

    def test_session_id_initialises_to_none(self):
        from ml.recommendation.main import SessionState
        assert SessionState().session_id is None

    def test_db_initialises_to_none(self):
        from ml.recommendation.main import SessionState
        assert SessionState().db is None

    def test_bq_client_initialises_to_none(self):
        from ml.recommendation.main import SessionState
        assert SessionState().bq_client is None

    def test_songs_df_initialises_to_none(self):
        from ml.recommendation.main import SessionState
        assert SessionState().songs_df is None

    def test_catalog_df_initialises_to_none(self):
        from ml.recommendation.main import SessionState
        assert SessionState().catalog_df is None

    def test_embedding_lookup_initialises_to_empty_dict(self):
        from ml.recommendation.main import SessionState
        assert SessionState().embedding_lookup == {}

    def test_gru_model_initialises_to_none(self):
        from ml.recommendation.main import SessionState
        assert SessionState().gru_model is None

    def test_user_ids_initialises_to_empty_list(self):
        from ml.recommendation.main import SessionState
        assert SessionState().user_ids == []

    def test_all_user_liked_initialises_to_empty_dict(self):
        from ml.recommendation.main import SessionState
        assert SessionState().all_user_liked == {}

    def test_last_batch_songs_initialises_to_empty_list(self):
        from ml.recommendation.main import SessionState
        assert SessionState().last_batch_songs == []

    def test_monitoring_writer_initialises_to_none(self):
        from ml.recommendation.main import SessionState
        assert SessionState().monitoring_writer is None

    def test_all_monitoring_attrs_present(self):
        from ml.recommendation.main import SessionState
        state = SessionState()
        assert hasattr(state, "catalog_df")
        assert hasattr(state, "last_batch_songs")
        assert hasattr(state, "monitoring_writer")


# ── _resolve_gru_weight ───────────────────────────────────────────────────────

class TestResolveGruWeight:

    def _call(self, songs_played, gru_active):
        from ml.recommendation.main import _resolve_gru_weight
        return _resolve_gru_weight(songs_played, gru_active)

    def test_returns_zero_when_gru_not_active(self):
        assert self._call(10, False) == 0.0

    def test_returns_zero_for_cold_start(self):
        assert self._call(0, False) == 0.0

    def test_returns_float_when_gru_active(self):
        result = self._call(6, True)
        assert isinstance(result, float)
        assert result > 0.0

    def test_ramp_song_4_lower_than_song_6(self):
        w4 = self._call(4, True)
        w6 = self._call(6, True)
        assert w4 <= w6

    def test_gru_weight_is_non_negative(self):
        for songs in range(10):
            assert self._call(songs, True) >= 0.0


# ── _aggregate_scores ─────────────────────────────────────────────────────────

class TestAggregateScores:

    def _call(self, cbf_df, cf_df, gru_df, gru_active, songs_played):
        from ml.recommendation.main import _aggregate_scores
        return _aggregate_scores(cbf_df, cf_df, gru_df, gru_active, songs_played)

    def make_cbf_df(self, n=5):
        np.random.seed(42)
        return pd.DataFrame({
            "video_id":      [f"vid_{i}" for i in range(n)],
            "track_title":   [f"Song {i}" for i in range(n)],
            "artist_name":   [f"Artist {i}" for i in range(n)],
            "genre":         ["pop"] * n,
            "cbf_score":     np.random.rand(n),
            "session_score": np.random.rand(n) * 0.5,
        })

    def make_cf_df(self, n=5):
        np.random.seed(1)
        return pd.DataFrame({
            "video_id": [f"vid_{i}" for i in range(n)],
            "cf_score":  np.random.rand(n),
        })

    def make_gru_df(self, n=5):
        np.random.seed(2)
        return pd.DataFrame({
            "video_id":  [f"vid_{i}" for i in range(n)],
            "gru_score": np.random.rand(n),
        })

    def test_returns_tuple(self):
        result = self._call(self.make_cbf_df(), self.make_cf_df(), pd.DataFrame(), False, 0)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_returns_dataframe_and_float(self):
        df, weight = self._call(self.make_cbf_df(), self.make_cf_df(), pd.DataFrame(), False, 0)
        assert isinstance(df, pd.DataFrame)
        assert isinstance(weight, float)

    def test_empty_cbf_returns_empty_dataframe(self):
        df, weight = self._call(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), False, 0)
        assert df.empty

    def test_cold_start_gru_score_is_zero(self):
        df, _ = self._call(self.make_cbf_df(), self.make_cf_df(), pd.DataFrame(), False, 0)
        assert (df["gru_score"] == 0.0).all()

    def test_cold_start_gru_weight_is_zero(self):
        _, gru_weight = self._call(self.make_cbf_df(), self.make_cf_df(), pd.DataFrame(), False, 2)
        assert gru_weight == 0.0

    def test_warm_gru_weight_is_positive(self):
        _, gru_weight = self._call(
            self.make_cbf_df(), self.make_cf_df(),
            self.make_gru_df(), True, 6
        )
        assert gru_weight > 0.0

    def test_warm_gru_scores_are_non_zero(self):
        df, _ = self._call(
            self.make_cbf_df(), self.make_cf_df(),
            self.make_gru_df(), True, 6
        )
        assert "gru_score" in df.columns
        assert df["gru_score"].sum() > 0.0

    def test_final_score_column_present(self):
        df, _ = self._call(self.make_cbf_df(), self.make_cf_df(), pd.DataFrame(), False, 0)
        assert "final_score" in df.columns

    def test_sorted_by_final_score_descending(self):
        df, _ = self._call(self.make_cbf_df(), self.make_cf_df(), pd.DataFrame(), False, 0)
        scores = df["final_score"].tolist()
        assert scores == sorted(scores, reverse=True)

    def test_no_more_than_top_n_results(self):
        from ml.recommendation.config import TOP_N
        cbf = self.make_cbf_df(n=50)
        # give each a unique video_id
        cbf["video_id"] = [f"vid_{i}" for i in range(50)]
        df, _ = self._call(cbf, pd.DataFrame(), pd.DataFrame(), False, 0)
        assert len(df) <= TOP_N

    def test_empty_cf_uses_zero_cf_score(self):
        df, _ = self._call(self.make_cbf_df(), pd.DataFrame(), pd.DataFrame(), False, 0)
        assert "cf_score" in df.columns
        assert (df["cf_score"] == 0.0).all()

    def test_session_score_column_added_if_missing(self):
        cbf = self.make_cbf_df()
        cbf = cbf.drop(columns=["session_score"])
        df, _ = self._call(cbf, pd.DataFrame(), pd.DataFrame(), False, 0)
        assert "final_score" in df.columns   # didn't crash

    def test_cf_scores_merged_by_video_id(self):
        cbf = self.make_cbf_df(n=3)
        cf  = pd.DataFrame({
            "video_id": ["vid_0", "vid_1", "vid_2"],
            "cf_score": [0.9, 0.5, 0.1],
        })
        df, _ = self._call(cbf, cf, pd.DataFrame(), False, 0)
        assert "cf_score" in df.columns
        assert df[df["video_id"] == "vid_0"]["cf_score"].iloc[0] == pytest.approx(0.9, abs=1e-4)


# ── _resolve_phase_name ───────────────────────────────────────────────────────

class TestResolvePhase:

    def _call(self, songs_played, gru_active):
        from ml.recommendation.main import _resolve_phase_name
        return _resolve_phase_name(songs_played, gru_active)

    def test_cold_start_when_gru_not_active(self):
        assert self._call(0, False) == "cold_start"

    def test_cold_start_for_first_three_songs(self):
        for n in range(3):
            assert self._call(n, False) == "cold_start"

    def test_gru_soft_entry_at_song_4(self):
        from ml.recommendation.config import GRU_WEIGHT_RAMP
        # find the songs_played value that maps to 0.20
        song_4 = next((k for k, v in GRU_WEIGHT_RAMP.items() if v == 0.20), 4)
        assert self._call(song_4, True) == "gru_soft_entry"

    def test_gru_growing_at_song_5(self):
        from ml.recommendation.config import GRU_WEIGHT_RAMP
        song_5 = next((k for k, v in GRU_WEIGHT_RAMP.items() if v == 0.25), 5)
        assert self._call(song_5, True) == "gru_growing"

    def test_full_hybrid_at_song_6_plus(self):
        assert self._call(6, True) == "full_hybrid"

    def test_full_hybrid_for_large_song_count(self):
        assert self._call(100, True) == "full_hybrid"

    def test_returns_string(self):
        assert isinstance(self._call(0, False), str)

    def test_phase_never_gru_when_inactive(self):
        for n in range(20):
            phase = self._call(n, False)
            assert "gru" not in phase or phase == "cold_start"


# ── _resolve_active_weights ───────────────────────────────────────────────────

class TestResolveActiveWeights:

    def _call(self, songs_played, gru_active, gru_weight):
        from ml.recommendation.main import _resolve_active_weights
        return _resolve_active_weights(songs_played, gru_active, gru_weight)

    def test_returns_dict(self):
        assert isinstance(self._call(0, False, 0.0), dict)

    def test_has_cbf_cf_gru_keys(self):
        w = self._call(0, False, 0.0)
        assert set(w.keys()) == {"cbf", "cf", "gru"}

    def test_cold_start_gru_weight_is_zero(self):
        assert self._call(0, False, 0.0)["gru"] == 0.0

    def test_cold_start_weights_sum_to_one(self):
        w = self._call(0, False, 0.0)
        assert abs(w["cbf"] + w["cf"] + w["gru"] - 1.0) < 0.01

    def test_warm_weights_sum_to_one(self):
        w = self._call(6, True, 0.3)
        assert abs(w["cbf"] + w["cf"] + w["gru"] - 1.0) < 0.01

    def test_warm_gru_weight_positive(self):
        assert self._call(6, True, 0.3)["gru"] > 0.0

    def test_cold_start_cbf_greater_than_cf(self):
        # cold start is CBF-heavy
        w = self._call(0, False, 0.0)
        assert w["cbf"] > w["cf"]

    def test_all_weights_between_zero_and_one(self):
        for gru_active, gru_w, songs in [(False, 0.0, 0), (True, 0.3, 6)]:
            w = self._call(songs, gru_active, gru_w)
            for key, val in w.items():
                assert 0.0 <= val <= 1.0, f"{key}={val} out of range"

    def test_weights_are_rounded_floats(self):
        w = self._call(6, True, 0.3)
        for val in w.values():
            assert isinstance(val, float)


# ── _build_scored_songs ───────────────────────────────────────────────────────

class TestBuildScoredSongs:

    def _call(self, df):
        from ml.recommendation.main import _build_scored_songs
        return _build_scored_songs(df)

    def test_returns_list(self):
        assert isinstance(self._call(make_recommendations_df()), list)

    def test_length_matches_dataframe(self):
        df = make_recommendations_df(n=5)
        assert len(self._call(df)) == 5

    def test_each_item_is_dict(self):
        for item in self._call(make_recommendations_df()):
            assert isinstance(item, dict)

    def test_track_title_mapped_to_title(self):
        df = make_recommendations_df(n=1)
        result = self._call(df)
        assert "title" in result[0]
        assert "track_title" not in result[0]

    def test_artist_name_mapped_to_artist(self):
        df = make_recommendations_df(n=1)
        result = self._call(df)
        assert "artist" in result[0]
        assert "artist_name" not in result[0]

    def test_title_value_correct(self):
        df = make_recommendations_df(n=1)
        df["track_title"] = ["My Song"]
        result = self._call(df)
        assert result[0]["title"] == "My Song"

    def test_artist_value_correct(self):
        df = make_recommendations_df(n=1)
        df["artist_name"] = ["My Artist"]
        result = self._call(df)
        assert result[0]["artist"] == "My Artist"

    def test_required_keys_present(self):
        required = {"video_id", "title", "artist", "genre",
                    "cbf_score", "cf_score", "gru_score", "final_score"}
        for item in self._call(make_recommendations_df(n=3)):
            assert required.issubset(item.keys())

    def test_popularity_score_not_included(self):
        # popularity_score is not in BigQuery — must not be in output
        for item in self._call(make_recommendations_df(n=3)):
            assert "popularity_score" not in item

    def test_scores_are_rounded_floats(self):
        result = self._call(make_recommendations_df(n=3))
        for item in result:
            for key in ["cbf_score", "cf_score", "gru_score", "final_score"]:
                assert isinstance(item[key], float)
                # 4 decimal places max
                assert item[key] == round(item[key], 4)

    def test_video_id_is_string(self):
        result = self._call(make_recommendations_df(n=3))
        for item in result:
            assert isinstance(item["video_id"], str)

    def test_empty_dataframe_returns_empty_list(self):
        empty = pd.DataFrame(columns=make_recommendations_df().columns)
        assert self._call(empty) == []

    def test_order_preserved_from_dataframe(self):
        df = make_recommendations_df(n=5)
        result = self._call(df)
        for i, item in enumerate(result):
            assert item["video_id"] == df.iloc[i]["video_id"]

    def test_is_json_serializable(self):
        import json
        result = self._call(make_recommendations_df(n=5))
        try:
            json.dumps(result)
        except (TypeError, ValueError) as e:
            pytest.fail(f"_build_scored_songs result not JSON-serializable: {e}")


# ── _write_recommendations_to_firestore ───────────────────────────────────────

class TestWriteRecommendationsToFirestore:

    def _call(self, db, session_id, recommendations, gru_active=False, gru_weight=0.0):
        from ml.recommendation.main import _write_recommendations_to_firestore
        return _write_recommendations_to_firestore(
            db, session_id, recommendations, gru_active, gru_weight
        )

    def test_clears_existing_recommendations(self):
        db   = make_mock_db()
        # simulate two existing docs
        doc1, doc2 = MagicMock(), MagicMock()
        db.collection.return_value \
          .document.return_value \
          .collection.return_value \
          .stream.return_value = [doc1, doc2]
        self._call(db, "sess_1", make_recommendations_df(n=3))
        doc1.reference.delete.assert_called_once()
        doc2.reference.delete.assert_called_once()

    def test_writes_correct_number_of_docs(self):
        db    = make_mock_db()
        n     = 5
        recs  = make_recommendations_df(n=n)
        set_mock = db.collection.return_value \
                     .document.return_value \
                     .collection.return_value \
                     .document.return_value \
                     .set
        self._call(db, "sess_1", recs)
        assert set_mock.call_count == n

    def test_first_doc_written_with_rank_1(self):
        db   = make_mock_db()
        recs = make_recommendations_df(n=3)
        doc_mock = db.collection.return_value \
                     .document.return_value \
                     .collection.return_value \
                     .document
        self._call(db, "sess_1", recs)
        doc_mock.assert_any_call("1")

    def test_gru_score_is_none_when_gru_inactive(self):
        db    = make_mock_db()
        recs  = make_recommendations_df(n=1, with_gru=False)
        set_mock = db.collection.return_value \
                     .document.return_value \
                     .collection.return_value \
                     .document.return_value \
                     .set
        self._call(db, "sess_1", recs, gru_active=False)
        written = set_mock.call_args[0][0]
        assert written["gru_score"] is None

    def test_gru_score_is_float_when_gru_active(self):
        db   = make_mock_db()
        recs = make_recommendations_df(n=1, with_gru=True)
        set_mock = db.collection.return_value \
                     .document.return_value \
                     .collection.return_value \
                     .document.return_value \
                     .set
        self._call(db, "sess_1", recs, gru_active=True, gru_weight=0.3)
        written = set_mock.call_args[0][0]
        assert written["gru_score"] is not None
        assert isinstance(written["gru_score"], float)

    def test_written_doc_has_required_fields(self):
        db    = make_mock_db()
        recs  = make_recommendations_df(n=1)
        set_mock = db.collection.return_value \
                     .document.return_value \
                     .collection.return_value \
                     .document.return_value \
                     .set
        self._call(db, "sess_1", recs)
        written = set_mock.call_args[0][0]
        required = {"video_id","track_title","artist_name","genre",
                    "cbf_score","cf_score","session_score","gru_score",
                    "final_score","rank","gru_active","gru_weight","recommended_at"}
        assert required.issubset(written.keys())

    def test_empty_recommendations_writes_nothing(self):
        db   = make_mock_db()
        set_mock = db.collection.return_value \
                     .document.return_value \
                     .collection.return_value \
                     .document.return_value \
                     .set
        self._call(db, "sess_1", pd.DataFrame(columns=make_recommendations_df().columns))
        set_mock.assert_not_called()


# ── initialize_session ────────────────────────────────────────────────────────

class TestInitializeSession:

    @patch("ml.recommendation.main.fetch_all_user_liked")
    @patch("ml.recommendation.main.load_gru_model")
    @patch("ml.recommendation.main.build_embedding_lookup")
    @patch("ml.recommendation.main.fetch_all_embeddings")
    @patch("ml.recommendation.main.get_session_user_ids")
    @patch("ml.recommendation.main.get_client")
    @patch("ml.recommendation.main.get_db")
    def _init(self, mock_get_db, mock_get_client, mock_user_ids,
              mock_embeddings, mock_lookup, mock_gru, mock_liked,
              songs_df=None, gru_raises=False):
        from ml.recommendation.main import initialize_session
        if songs_df is None:
            songs_df = make_songs_df()
        mock_embeddings.return_value = songs_df
        mock_lookup.return_value     = {}
        mock_user_ids.return_value   = ["u1", "u2"]
        mock_liked.return_value      = {"u1": ["vid_0"]}
        if gru_raises:
            mock_gru.side_effect = Exception("model not found")
        else:
            mock_gru.return_value = MagicMock()
        mock_get_db.return_value     = make_mock_db()
        mock_get_client.return_value = MagicMock()
        return initialize_session("sess_1")

    def test_returns_session_state(self):
        from ml.recommendation.main import SessionState
        state = self._init()
        assert isinstance(state, SessionState)

    def test_session_id_set(self):
        assert self._init().session_id == "sess_1"

    def test_db_set(self):
        assert self._init().db is not None

    def test_bq_client_set(self):
        assert self._init().bq_client is not None

    def test_songs_df_loaded(self):
        songs_df = make_songs_df(n=20)
        state    = self._init(songs_df=songs_df)
        assert len(state.songs_df) == 20

    def test_catalog_df_equals_songs_df(self):
        # catalog_df is the monitoring alias — must be the exact same object
        state = self._init()
        assert state.catalog_df is state.songs_df

    def test_embedding_lookup_set(self):
        state = self._init()
        assert state.embedding_lookup is not None

    def test_gru_model_loaded_when_available(self):
        state = self._init(gru_raises=False)
        assert state.gru_model is not None

    def test_gru_model_none_when_load_fails(self):
        # GRU failure is non-fatal — must fall back gracefully
        state = self._init(gru_raises=True)
        assert state.gru_model is None

    def test_user_ids_populated(self):
        state = self._init()
        assert state.user_ids == ["u1", "u2"]

    def test_all_user_liked_populated(self):
        state = self._init()
        assert len(state.all_user_liked) > 0

    def test_last_batch_songs_is_empty_list(self):
        state = self._init()
        assert state.last_batch_songs == []

    def test_monitoring_writer_is_none(self):
        # monitoring_writer is set by ml_trigger.py AFTER init — must be None here
        state = self._init()
        assert state.monitoring_writer is None


# ── recommend_next_song ───────────────────────────────────────────────────────

class TestRecommendNextSong:
    """
    recommend_next_song() is the core recommendation cycle.
    All external I/O (Firestore, BigQuery, GRU) is mocked.
    Monitoring side effects (last_batch_songs, write_rec_snapshot)
    are tested explicitly.
    """

    def _patch_all(self):
        """Returns a context manager that patches all external calls."""
        return [
            patch("ml.recommendation.main.get_session_play_sequence"),
            patch("ml.recommendation.main.get_session_track_scores"),
            patch("ml.recommendation.main.get_already_played_ids"),
            patch("ml.recommendation.main.get_user_liked_songs"),
            patch("ml.recommendation.main.get_user_disliked_songs"),
            patch("ml.recommendation.main.build_user_vectors"),
            patch("ml.recommendation.main.build_global_vector"),
            patch("ml.recommendation.main.get_cbf_scores"),
            patch("ml.recommendation.main.get_cf_scores"),
            patch("ml.recommendation.main.get_gru_scores"),
            patch("ml.recommendation.main._write_recommendations_to_firestore"),
        ]

    def _run(self, state, songs_played=0, gru_active=False, recs_df=None):
        """
        Runs recommend_next_song with all external calls mocked.
        Returns (result, mocks_dict).
        """
        from ml.recommendation.main import recommend_next_song
        recs_df = recs_df or make_recommendations_df(n=5, with_gru=gru_active)
        play_seq = [f"vid_{i}" for i in range(songs_played)]

        patches = self._patch_all()
        mocks   = [p.start() for p in patches]
        try:
            (mock_play_seq, mock_track_scores, mock_already_played,
             mock_user_liked, mock_user_disliked, mock_build_vectors,
             mock_global_vec, mock_cbf, mock_cf, mock_gru, mock_write) = mocks

            mock_play_seq.return_value    = play_seq
            mock_track_scores.return_value = {}
            mock_already_played.return_value = set()
            mock_user_liked.return_value   = {"u1": ["vid_0"]}
            mock_user_disliked.return_value = {}
            mock_build_vectors.return_value = {"u1": np.random.rand(386)}
            mock_global_vec.return_value    = np.random.rand(1, 386)
            mock_cbf.return_value           = recs_df
            mock_cf.return_value            = pd.DataFrame()
            mock_gru.return_value           = pd.DataFrame()

            result = recommend_next_song(state)
        finally:
            for p in patches:
                p.stop()

        return result

    def test_returns_dict(self):
        state = make_mock_state()
        result = self._run(state)
        assert isinstance(result, dict)

    def test_returns_top_song(self):
        state = make_mock_state()
        result = self._run(state)
        assert "track_title" in result or "video_id" in result

    def test_returns_empty_dict_when_no_recommendations(self):
        from ml.recommendation.main import recommend_next_song
        state = make_mock_state()
        empty = pd.DataFrame(columns=make_recommendations_df().columns)

        patches = self._patch_all()
        mocks   = [p.start() for p in patches]
        try:
            (mock_play_seq, mock_track_scores, mock_already_played,
             mock_user_liked, mock_user_disliked, mock_build_vectors,
             mock_global_vec, mock_cbf, mock_cf, mock_gru, mock_write) = mocks
            mock_play_seq.return_value     = []
            mock_track_scores.return_value = {}
            mock_already_played.return_value = set()
            mock_user_liked.return_value   = {"u1": ["vid_0"]}
            mock_user_disliked.return_value = {}
            mock_build_vectors.return_value = {"u1": np.random.rand(386)}
            mock_global_vec.return_value    = np.random.rand(1, 386)
            mock_cbf.return_value           = empty
            mock_cf.return_value            = pd.DataFrame()
            mock_gru.return_value           = pd.DataFrame()
            result = recommend_next_song(state)
        finally:
            for p in patches:
                p.stop()

        assert result == {}

    def test_sets_last_batch_songs_on_state(self):
        state = make_mock_state()
        self._run(state)
        assert isinstance(state.last_batch_songs, list)
        assert len(state.last_batch_songs) > 0

    def test_last_batch_songs_have_correct_keys(self):
        state = make_mock_state()
        self._run(state)
        required = {"video_id","title","artist","genre",
                    "cbf_score","cf_score","gru_score","final_score"}
        for item in state.last_batch_songs:
            assert required.issubset(item.keys())

    def test_last_batch_songs_uses_title_not_track_title(self):
        state = make_mock_state()
        self._run(state)
        for item in state.last_batch_songs:
            assert "title" in item
            assert "track_title" not in item

    def test_last_batch_songs_uses_artist_not_artist_name(self):
        state = make_mock_state()
        self._run(state)
        for item in state.last_batch_songs:
            assert "artist" in item
            assert "artist_name" not in item

    def test_write_rec_snapshot_called_when_writer_set(self):
        state = make_mock_state()
        writer = make_mock_monitoring_writer()
        state.monitoring_writer = writer
        self._run(state)
        writer.write_rec_snapshot.assert_called_once()

    def test_write_rec_snapshot_not_called_when_writer_none(self):
        state = make_mock_state()
        state.monitoring_writer = None
        writer = make_mock_monitoring_writer()
        self._run(state)
        writer.write_rec_snapshot.assert_not_called()

    def test_write_rec_snapshot_receives_phase_string(self):
        state = make_mock_state()
        writer = make_mock_monitoring_writer()
        state.monitoring_writer = writer
        self._run(state, songs_played=0)
        call_kwargs = writer.write_rec_snapshot.call_args[1]
        assert isinstance(call_kwargs.get("phase"), str)

    def test_write_rec_snapshot_receives_weights_dict(self):
        state = make_mock_state()
        writer = make_mock_monitoring_writer()
        state.monitoring_writer = writer
        self._run(state)
        call_kwargs = writer.write_rec_snapshot.call_args[1]
        weights = call_kwargs.get("weights")
        assert isinstance(weights, dict)
        assert set(weights.keys()) == {"cbf", "cf", "gru"}

    def test_write_rec_snapshot_receives_scored_songs_list(self):
        state = make_mock_state()
        writer = make_mock_monitoring_writer()
        state.monitoring_writer = writer
        self._run(state)
        call_kwargs = writer.write_rec_snapshot.call_args[1]
        assert isinstance(call_kwargs.get("scored_songs"), list)

    def test_monitoring_exception_does_not_crash_cycle(self):
        state = make_mock_state()
        writer = make_mock_monitoring_writer()
        writer.write_rec_snapshot.side_effect = Exception("Firestore down")
        state.monitoring_writer = writer
        # must NOT raise — monitoring is non-critical
        result = self._run(state)
        assert isinstance(result, dict)

    def test_cold_start_phase_when_no_songs_played(self):
        state = make_mock_state()
        writer = make_mock_monitoring_writer()
        state.monitoring_writer = writer
        self._run(state, songs_played=0, gru_active=False)
        call_kwargs = writer.write_rec_snapshot.call_args[1]
        assert call_kwargs["phase"] == "cold_start"

    def test_write_recommendations_to_firestore_called(self):
        from ml.recommendation.main import recommend_next_song
        state = make_mock_state()

        patches = self._patch_all()
        mocks   = [p.start() for p in patches]
        try:
            (mock_play_seq, mock_track_scores, mock_already_played,
             mock_user_liked, mock_user_disliked, mock_build_vectors,
             mock_global_vec, mock_cbf, mock_cf, mock_gru, mock_write) = mocks
            mock_play_seq.return_value     = []
            mock_track_scores.return_value = {}
            mock_already_played.return_value = set()
            mock_user_liked.return_value   = {"u1": ["vid_0"]}
            mock_user_disliked.return_value = {}
            mock_build_vectors.return_value = {"u1": np.random.rand(386)}
            mock_global_vec.return_value    = np.random.rand(1, 386)
            mock_cbf.return_value           = make_recommendations_df(n=5)
            mock_cf.return_value            = pd.DataFrame()
            mock_gru.return_value           = pd.DataFrame()
            recommend_next_song(state)
            mock_write.assert_called_once()
        finally:
            for p in patches:
                p.stop()

    def test_monitoring_block_runs_before_firestore_write(self):
        """last_batch_songs must be set before the frontend write happens."""
        from ml.recommendation.main import recommend_next_song
        state = make_mock_state()
        call_order = []

        writer = make_mock_monitoring_writer()
        writer.write_rec_snapshot.side_effect = lambda **kw: call_order.append("monitoring")
        state.monitoring_writer = writer

        patches = self._patch_all()
        mocks   = [p.start() for p in patches]
        try:
            (mock_play_seq, mock_track_scores, mock_already_played,
             mock_user_liked, mock_user_disliked, mock_build_vectors,
             mock_global_vec, mock_cbf, mock_cf, mock_gru, mock_write) = mocks
            mock_play_seq.return_value     = []
            mock_track_scores.return_value = {}
            mock_already_played.return_value = set()
            mock_user_liked.return_value   = {"u1": ["vid_0"]}
            mock_user_disliked.return_value = {}
            mock_build_vectors.return_value = {"u1": np.random.rand(386)}
            mock_global_vec.return_value    = np.random.rand(1, 386)
            mock_cbf.return_value           = make_recommendations_df(n=5)
            mock_cf.return_value            = pd.DataFrame()
            mock_gru.return_value           = pd.DataFrame()
            mock_write.side_effect          = lambda **kw: call_order.append("firestore")
            recommend_next_song(state)
        finally:
            for p in patches:
                p.stop()

        assert "monitoring" in call_order
        assert "firestore" in call_order
        assert call_order.index("monitoring") < call_order.index("firestore")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
