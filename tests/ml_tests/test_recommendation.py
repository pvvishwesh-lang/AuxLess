"""
Tests for the recommendation system.

Covers:
  - CBF.py          → get_cbf_scores, _normalize_session_scores, _build_exclusion_set
  - user_vectors.py → build_user_vectors, build_global_vector, get_already_played_ids
  - main.py         → _resolve_gru_weight, _aggregate_scores
  - config.py       → GRU_WEIGHT_RAMP, thresholds, weights

All tests run in CI (no GCP calls).
Tests that require live GCP are skipped in CI.
"""

import os
import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_songs_df():
    """
    Small in-memory song catalog with 386-dim embeddings.
    Simulates what BigQuery returns.
    """
    np.random.seed(42)
    songs = []
    for i in range(20):
        songs.append({
            "video_id":    f"vid_{i}",
            "track_title": f"Song {i}",
            "artist_name": f"Artist {i % 5}",
            "genre":       ["pop", "rock", "jazz", "hip-hop"][i % 4],
            "embedding":   np.random.rand(386).astype(np.float32),
        })
    return pd.DataFrame(songs)


@pytest.fixture
def sample_user_liked_songs():
    return {
        "user_1": ["vid_0", "vid_1", "vid_2"],
        "user_2": ["vid_3", "vid_4"],
    }


@pytest.fixture
def sample_user_disliked_songs():
    return {
        "user_1": ["vid_5"],
        "user_2": ["vid_6"],
    }


@pytest.fixture
def sample_track_scores():
    return {
        "vid_7":  {"score":  2.0, "like_count": 1, "skip_count": 0,
                   "dislike_count": 0, "replay_count": 0, "last_updated": None},
        "vid_8":  {"score": -1.0, "like_count": 0, "skip_count": 1,
                   "dislike_count": 0, "replay_count": 0, "last_updated": None},
        "vid_9":  {"score":  0.0, "like_count": 0, "skip_count": 0,
                   "dislike_count": 0, "replay_count": 0, "last_updated": None},
    }


@pytest.fixture
def global_vector():
    np.random.seed(0)
    v = np.random.rand(1, 386).astype(np.float32)
    v = v / np.linalg.norm(v)
    return v


# ── Config tests ──────────────────────────────────────────────────────────────

class TestConfig:

    def test_gru_weight_ramp_keys(self):
        from ml.recommendation.config import GRU_WEIGHT_RAMP
        assert 4 in GRU_WEIGHT_RAMP
        assert 5 in GRU_WEIGHT_RAMP
        assert 6 in GRU_WEIGHT_RAMP

    def test_gru_weight_ramp_values_increasing(self):
        from ml.recommendation.config import GRU_WEIGHT_RAMP
        weights = [GRU_WEIGHT_RAMP[k] for k in sorted(GRU_WEIGHT_RAMP)]
        assert weights == sorted(weights), "GRU ramp weights should be non-decreasing"

    def test_cold_weights_sum_to_one(self):
        from ml.recommendation.config import WEIGHT_CBF_COLD, WEIGHT_CF_COLD
        assert abs(WEIGHT_CBF_COLD + WEIGHT_CF_COLD - 1.0) < 1e-6

    def test_warm_weights_sum_to_one(self):
        from ml.recommendation.config import (
            WEIGHT_CBF_WARM, WEIGHT_CF_WARM, WEIGHT_GRU_WARM
        )
        assert abs(WEIGHT_CBF_WARM + WEIGHT_CF_WARM + WEIGHT_GRU_WARM - 1.0) < 1e-6

    def test_top_n_matches_batch_size(self):
        from ml.recommendation.config import TOP_N, RECOMMENDATION_BATCH_SIZE
        assert TOP_N == RECOMMENDATION_BATCH_SIZE

    def test_gru_activation_threshold(self):
        from ml.recommendation.config import GRU_ACTIVATION_THRESHOLD
        assert GRU_ACTIVATION_THRESHOLD >= 1

    def test_refresh_threshold_positive(self):
        from ml.recommendation.config import REFRESH_THRESHOLD
        assert REFRESH_THRESHOLD > 0

    def test_embedding_dim(self):
        from ml.recommendation.config import GRU_EMBEDDING_DIM
        assert GRU_EMBEDDING_DIM == 386


# ── CBF tests ─────────────────────────────────────────────────────────────────

class TestCBF:

    def test_returns_dataframe(
        self, global_vector, sample_songs_df,
        sample_user_liked_songs, sample_user_disliked_songs,
        sample_track_scores
    ):
        from ml.recommendation.CBF import get_cbf_scores
        result = get_cbf_scores(
            global_vector=global_vector,
            songs_df=sample_songs_df,
            user_liked_songs=sample_user_liked_songs,
            user_disliked_songs=sample_user_disliked_songs,
            already_played_ids=set(),
            track_scores=sample_track_scores,
        )
        assert isinstance(result, pd.DataFrame)

    def test_returns_correct_columns(
        self, global_vector, sample_songs_df,
        sample_user_liked_songs, sample_user_disliked_songs,
        sample_track_scores
    ):
        from ml.recommendation.CBF import get_cbf_scores
        result = get_cbf_scores(
            global_vector=global_vector,
            songs_df=sample_songs_df,
            user_liked_songs=sample_user_liked_songs,
            user_disliked_songs=sample_user_disliked_songs,
            already_played_ids=set(),
            track_scores=sample_track_scores,
        )
        expected_cols = {
            "video_id", "track_title", "artist_name",
            "genre", "cbf_score", "session_score", "final_score"
        }
        assert expected_cols.issubset(set(result.columns))

    def test_excludes_liked_songs(
        self, global_vector, sample_songs_df,
        sample_user_liked_songs, sample_user_disliked_songs
    ):
        from ml.recommendation.CBF import get_cbf_scores
        result = get_cbf_scores(
            global_vector=global_vector,
            songs_df=sample_songs_df,
            user_liked_songs=sample_user_liked_songs,
            user_disliked_songs=sample_user_disliked_songs,
            already_played_ids=set(),
            track_scores={},
        )
        all_liked = {
            vid for ids in sample_user_liked_songs.values() for vid in ids
        }
        assert len(set(result["video_id"]) & all_liked) == 0

    def test_excludes_disliked_songs(
        self, global_vector, sample_songs_df,
        sample_user_liked_songs, sample_user_disliked_songs
    ):
        from ml.recommendation.CBF import get_cbf_scores
        result = get_cbf_scores(
            global_vector=global_vector,
            songs_df=sample_songs_df,
            user_liked_songs=sample_user_liked_songs,
            user_disliked_songs=sample_user_disliked_songs,
            already_played_ids=set(),
            track_scores={},
        )
        all_disliked = {
            vid for ids in sample_user_disliked_songs.values() for vid in ids
        }
        assert len(set(result["video_id"]) & all_disliked) == 0

    def test_excludes_already_played(
        self, global_vector, sample_songs_df
    ):
        from ml.recommendation.CBF import get_cbf_scores
        already_played = {"vid_10", "vid_11", "vid_12"}
        result = get_cbf_scores(
            global_vector=global_vector,
            songs_df=sample_songs_df,
            user_liked_songs={},
            user_disliked_songs={},
            already_played_ids=already_played,
            track_scores={},
        )
        assert len(set(result["video_id"]) & already_played) == 0

    def test_sorted_by_final_score_descending(
        self, global_vector, sample_songs_df
    ):
        from ml.recommendation.CBF import get_cbf_scores
        result = get_cbf_scores(
            global_vector=global_vector,
            songs_df=sample_songs_df,
            user_liked_songs={},
            user_disliked_songs={},
            already_played_ids=set(),
            track_scores={},
        )
        scores = result["final_score"].tolist()
        assert scores == sorted(scores, reverse=True)

    def test_empty_catalog_returns_empty_df(self, global_vector):
        from ml.recommendation.CBF import get_cbf_scores
        empty_df = pd.DataFrame(
            columns=["video_id", "track_title", "artist_name", "genre", "embedding"]
        )
        result = get_cbf_scores(
            global_vector=global_vector,
            songs_df=empty_df,
            user_liked_songs={},
            user_disliked_songs={},
            already_played_ids=set(),
            track_scores={},
        )
        assert result.empty

    def test_all_excluded_returns_empty_df(
        self, global_vector, sample_songs_df
    ):
        from ml.recommendation.CBF import get_cbf_scores
        all_ids = set(sample_songs_df["video_id"].tolist())
        result = get_cbf_scores(
            global_vector=global_vector,
            songs_df=sample_songs_df,
            user_liked_songs={"user_1": list(all_ids)},
            user_disliked_songs={},
            already_played_ids=set(),
            track_scores={},
        )
        assert result.empty

    def test_cbf_scores_between_neg1_and_1(
        self, global_vector, sample_songs_df
    ):
        from ml.recommendation.CBF import get_cbf_scores
        result = get_cbf_scores(
            global_vector=global_vector,
            songs_df=sample_songs_df,
            user_liked_songs={},
            user_disliked_songs={},
            already_played_ids=set(),
            track_scores={},
        )
        assert result["cbf_score"].between(-1.0, 1.0).all()

    def test_session_score_nudges_final_score(
        self, global_vector, sample_songs_df
    ):
        from ml.recommendation.CBF import get_cbf_scores
        # with no track scores
        result_no_scores = get_cbf_scores(
            global_vector=global_vector,
            songs_df=sample_songs_df,
            user_liked_songs={},
            user_disliked_songs={},
            already_played_ids=set(),
            track_scores={},
        )
        # with positive track score on one song
        liked_vid = sample_songs_df["video_id"].iloc[0]
        result_with_scores = get_cbf_scores(
            global_vector=global_vector,
            songs_df=sample_songs_df,
            user_liked_songs={},
            user_disliked_songs={},
            already_played_ids=set(),
            track_scores={liked_vid: {
                "score": 2.0, "like_count": 1, "skip_count": 0,
                "dislike_count": 0, "replay_count": 0, "last_updated": None
            }},
        )
        # final scores should differ when session feedback is applied
        assert not result_no_scores["final_score"].equals(
            result_with_scores["final_score"]
        )


class TestNormalizeSessionScores:

    def test_normalizes_to_range(self):
        from ml.recommendation.CBF import _normalize_session_scores
        track_scores = {
            "a": {"score":  4.0},
            "b": {"score": -2.0},
            "c": {"score":  0.5},
        }
        result = _normalize_session_scores(track_scores)
        for v in result.values():
            assert -1.0 <= v <= 1.0

    def test_max_value_is_1(self):
        from ml.recommendation.CBF import _normalize_session_scores
        track_scores = {
            "a": {"score": 4.0},
            "b": {"score": 2.0},
        }
        result = _normalize_session_scores(track_scores)
        assert result["a"] == 1.0

    def test_empty_returns_empty(self):
        from ml.recommendation.CBF import _normalize_session_scores
        assert _normalize_session_scores({}) == {}

    def test_all_zeros_returns_zeros(self):
        from ml.recommendation.CBF import _normalize_session_scores
        track_scores = {
            "a": {"score": 0.0},
            "b": {"score": 0.0},
        }
        result = _normalize_session_scores(track_scores)
        assert all(v == 0.0 for v in result.values())


class TestBuildExclusionSet:

    def test_combines_all_sources(self):
        from ml.recommendation.CBF import _build_exclusion_set
        result = _build_exclusion_set(
            user_liked_songs    = {"u1": ["a", "b"]},
            user_disliked_songs = {"u1": ["c"]},
            already_played_ids  = {"d"},
        )
        assert result == {"a", "b", "c", "d"}

    def test_empty_inputs(self):
        from ml.recommendation.CBF import _build_exclusion_set
        result = _build_exclusion_set({}, {}, set())
        assert result == set()

    def test_deduplicates_overlapping_ids(self):
        from ml.recommendation.CBF import _build_exclusion_set
        result = _build_exclusion_set(
            user_liked_songs    = {"u1": ["a"]},
            user_disliked_songs = {"u1": ["a"]},
            already_played_ids  = {"a"},
        )
        assert result == {"a"}


# ── User vectors tests ────────────────────────────────────────────────────────

class TestUserVectors:

    def test_build_user_vectors_returns_dict(
        self, sample_songs_df, sample_user_liked_songs,
        sample_user_disliked_songs
    ):
        from ml.recommendation.user_vectors import build_user_vectors
        result = build_user_vectors(
            sample_user_liked_songs,
            sample_user_disliked_songs,
            sample_songs_df,
        )
        assert isinstance(result, dict)
        assert len(result) == len(sample_user_liked_songs)

    def test_vectors_are_unit_normalized(
        self, sample_songs_df, sample_user_liked_songs,
        sample_user_disliked_songs
    ):
        from ml.recommendation.user_vectors import build_user_vectors
        result = build_user_vectors(
            sample_user_liked_songs,
            sample_user_disliked_songs,
            sample_songs_df,
        )
        for uid, data in result.items():
            norm = np.linalg.norm(data["vector"])
            assert abs(norm - 1.0) < 1e-5, f"Vector for {uid} is not unit normalized"

    def test_vectors_have_correct_shape(
        self, sample_songs_df, sample_user_liked_songs,
        sample_user_disliked_songs
    ):
        from ml.recommendation.user_vectors import build_user_vectors
        result = build_user_vectors(
            sample_user_liked_songs,
            sample_user_disliked_songs,
            sample_songs_df,
        )
        for uid, data in result.items():
            assert data["vector"].shape == (386,)

    def test_liked_count_correct(
        self, sample_songs_df, sample_user_liked_songs,
        sample_user_disliked_songs
    ):
        from ml.recommendation.user_vectors import build_user_vectors
        result = build_user_vectors(
            sample_user_liked_songs,
            sample_user_disliked_songs,
            sample_songs_df,
        )
        # user_1 has 3 liked songs all in catalog
        assert result["user_1"]["liked_count"] == 3

    def test_skips_user_with_no_catalog_matches(self, sample_songs_df):
        from ml.recommendation.user_vectors import build_user_vectors
        result = build_user_vectors(
            user_liked_songs    = {"ghost_user": ["nonexistent_vid"]},
            user_disliked_songs = {},
            songs_df            = sample_songs_df,
        )
        assert "ghost_user" not in result

    def test_no_liked_songs_returns_empty(self, sample_songs_df):
        from ml.recommendation.user_vectors import build_user_vectors
        result = build_user_vectors({}, {}, sample_songs_df)
        assert result == {}

    def test_dislike_pushes_vector_away(self, sample_songs_df):
        from ml.recommendation.user_vectors import build_user_vectors
        # same liked songs, different disliked songs
        liked = {"u1": ["vid_0", "vid_1"]}

        result_no_dislike = build_user_vectors(liked, {}, sample_songs_df)
        result_with_dislike = build_user_vectors(
            liked, {"u1": ["vid_2"]}, sample_songs_df
        )
        # vectors should differ when disliked songs are applied
        v1 = result_no_dislike["u1"]["vector"]
        v2 = result_with_dislike["u1"]["vector"]
        assert not np.allclose(v1, v2)


class TestBuildGlobalVector:

    def test_returns_correct_shape(
        self, sample_songs_df, sample_user_liked_songs,
        sample_user_disliked_songs
    ):
        from ml.recommendation.user_vectors import build_user_vectors, build_global_vector
        user_vectors  = build_user_vectors(
            sample_user_liked_songs, sample_user_disliked_songs, sample_songs_df
        )
        global_vector = build_global_vector(user_vectors)
        assert global_vector.shape == (1, 386)

    def test_is_unit_normalized(
        self, sample_songs_df, sample_user_liked_songs,
        sample_user_disliked_songs
    ):
        from ml.recommendation.user_vectors import build_user_vectors, build_global_vector
        user_vectors  = build_user_vectors(
            sample_user_liked_songs, sample_user_disliked_songs, sample_songs_df
        )
        global_vector = build_global_vector(user_vectors)
        norm = np.linalg.norm(global_vector)
        assert abs(norm - 1.0) < 1e-5

    def test_raises_on_empty_user_vectors(self):
        from ml.recommendation.user_vectors import build_global_vector
        with pytest.raises(ValueError):
            build_global_vector({})

    def test_single_user_equals_user_vector(self, sample_songs_df):
        from ml.recommendation.user_vectors import build_user_vectors, build_global_vector
        user_vectors  = build_user_vectors(
            {"u1": ["vid_0", "vid_1"]}, {}, sample_songs_df
        )
        global_vector = build_global_vector(user_vectors)
        user_vec      = user_vectors["u1"]["vector"]
        assert np.allclose(global_vector.flatten(), user_vec, atol=1e-5)

    def test_higher_liked_count_weighs_more(self, sample_songs_df):
        from ml.recommendation.user_vectors import build_user_vectors, build_global_vector
        # u1 has more liked songs → should dominate the global vector
        user_vectors = build_user_vectors(
            {"u1": ["vid_0", "vid_1", "vid_2", "vid_3"], "u2": ["vid_4"]},
            {},
            sample_songs_df,
        )
        global_vector = build_global_vector(user_vectors)
        # global vector should be closer to u1's vector than u2's
        sim_u1 = np.dot(
            global_vector.flatten(),
            user_vectors["u1"]["vector"]
        )
        sim_u2 = np.dot(
            global_vector.flatten(),
            user_vectors["u2"]["vector"]
        )
        assert sim_u1 > sim_u2


class TestGetAlreadyPlayedIds:

    def test_extracts_video_ids(self):
        from ml.recommendation.user_vectors import get_already_played_ids
        play_sequence = [
            {"video_id": "abc", "play_order": 1, "liked_flag": 1},
            {"video_id": "xyz", "play_order": 2, "liked_flag": 0},
        ]
        result = get_already_played_ids(play_sequence)
        assert result == {"abc", "xyz"}

    def test_empty_sequence_returns_empty_set(self):
        from ml.recommendation.user_vectors import get_already_played_ids
        assert get_already_played_ids([]) == set()

    def test_returns_set_not_list(self):
        from ml.recommendation.user_vectors import get_already_played_ids
        result = get_already_played_ids([
            {"video_id": "a", "play_order": 1, "liked_flag": 0}
        ])
        assert isinstance(result, set)


# ── main.py unit tests (no GCP) ───────────────────────────────────────────────

class TestResolveGruWeight:

    def test_returns_zero_when_gru_inactive(self):
        from ml.recommendation.main import _resolve_gru_weight
        assert _resolve_gru_weight(songs_played=5, gru_active=False) == 0.0

    def test_returns_ramp_value_for_song_4(self):
        from ml.recommendation.main import _resolve_gru_weight
        from ml.recommendation.config import GRU_WEIGHT_RAMP
        result = _resolve_gru_weight(songs_played=4, gru_active=True)
        assert result == GRU_WEIGHT_RAMP[4]

    def test_returns_ramp_value_for_song_5(self):
        from ml.recommendation.main import _resolve_gru_weight
        from ml.recommendation.config import GRU_WEIGHT_RAMP
        result = _resolve_gru_weight(songs_played=5, gru_active=True)
        assert result == GRU_WEIGHT_RAMP[5]

    def test_returns_full_weight_for_song_6_plus(self):
        from ml.recommendation.main import _resolve_gru_weight
        from ml.recommendation.config import WEIGHT_GRU_WARM
        for songs_played in [6, 7, 10, 20]:
            result = _resolve_gru_weight(songs_played=songs_played, gru_active=True)
            assert result == WEIGHT_GRU_WARM


class TestAggregateScores:

    def _make_cbf_df(self, n=10):
        np.random.seed(1)
        return pd.DataFrame({
            "video_id":      [f"vid_{i}" for i in range(n)],
            "track_title":   [f"Song {i}" for i in range(n)],
            "artist_name":   [f"Artist {i}" for i in range(n)],
            "genre":         ["pop"] * n,
            "cbf_score":     np.random.rand(n),
            "session_score": np.random.uniform(-0.5, 0.5, n),
            "final_score":   np.random.rand(n),
        })

    def test_cold_start_sets_gru_score_to_zero(self):
        from ml.recommendation.main import _aggregate_scores
        cbf_df = self._make_cbf_df()
        result, gru_weight = _aggregate_scores(
            cbf_df=cbf_df,
            gru_df=pd.DataFrame(),
            gru_active=False,
            songs_played=2,
        )
        assert (result["gru_score"] == 0.0).all()
        assert gru_weight == 0.0

    def test_warm_merges_gru_scores(self):
        from ml.recommendation.main import _aggregate_scores
        cbf_df = self._make_cbf_df()
        gru_df = pd.DataFrame({
            "video_id":  [f"vid_{i}" for i in range(10)],
            "track_title": [f"Song {i}" for i in range(10)],
            "artist_name": [f"Artist {i}" for i in range(10)],
            "genre":     ["pop"] * 10,
            "gru_score": np.random.rand(10),
        })
        result, gru_weight = _aggregate_scores(
            cbf_df=cbf_df,
            gru_df=gru_df,
            gru_active=True,
            songs_played=6,
        )
        assert gru_weight > 0.0
        assert (result["gru_score"] >= 0.0).all()

    def test_empty_cbf_returns_empty(self):
        from ml.recommendation.main import _aggregate_scores
        result, gru_weight = _aggregate_scores(
            cbf_df=pd.DataFrame(),
            gru_df=pd.DataFrame(),
            gru_active=False,
            songs_played=1,
        )
        assert result.empty
        assert gru_weight == 0.0

    def test_session_score_included_in_final(self):
        from ml.recommendation.main import _aggregate_scores
        cbf_df = self._make_cbf_df()
        # force session_score to zero
        cbf_df_no_session             = cbf_df.copy()
        cbf_df_no_session["session_score"] = 0.0
        # force session_score to non-zero
        cbf_df_with_session             = cbf_df.copy()
        cbf_df_with_session["session_score"] = 0.5

        result_no, _   = _aggregate_scores(cbf_df_no_session,   pd.DataFrame(), False, 2)
        result_with, _ = _aggregate_scores(cbf_df_with_session, pd.DataFrame(), False, 2)

        # final scores must differ when session_score differs
        assert not np.allclose(
            result_no["final_score"].values,
            result_with["final_score"].values,
        )

    def test_result_sorted_by_final_score(self):
        from ml.recommendation.main import _aggregate_scores
        cbf_df = self._make_cbf_df()
        result, _ = _aggregate_scores(
            cbf_df=cbf_df,
            gru_df=pd.DataFrame(),
            gru_active=False,
            songs_played=2,
        )
        scores = result["final_score"].tolist()
        assert scores == sorted(scores, reverse=True)

    def test_result_capped_at_top_n(self):
        from ml.recommendation.main import _aggregate_scores
        from ml.recommendation.config import TOP_N
        cbf_df = self._make_cbf_df(n=50)
        result, _ = _aggregate_scores(
            cbf_df=cbf_df,
            gru_df=pd.DataFrame(),
            gru_active=False,
            songs_played=2,
        )
        assert len(result) <= TOP_N


# ── Live GCP tests (skipped in CI) ────────────────────────────────────────────

@pytest.mark.skipif(os.getenv("CI") == "true", reason="Skipped in CI — requires GCP")
def test_bigquery_fetch_live():
    from ml.recommendation.bigquery_client import get_client, fetch_all_embeddings
    client = get_client()
    df     = fetch_all_embeddings(client)
    assert not df.empty
    assert "embedding" in df.columns
    assert len(df.iloc[0]["embedding"]) == 386


@pytest.mark.skipif(os.getenv("CI") == "true", reason="Skipped in CI — requires GCP")
def test_firestore_session_users_live():
    import os
    from ml.recommendation.firestore_client import get_db, get_session_user_ids
    from ml.recommendation.config import PROJECT_ID, DATABASE_ID
    db       = get_db(PROJECT_ID, DATABASE_ID)
    session_id = os.getenv("TEST_SESSION_ID", "test-session-001")
    user_ids = get_session_user_ids(db, session_id)
    assert isinstance(user_ids, list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
