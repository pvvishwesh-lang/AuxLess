"""
Unit tests for ml.recommendation.cf.CF

All tests run without any GCP dependencies.
The co-occurrence index is built from small, hand-crafted datasets
so every assertion is deterministic and human-verifiable.
"""

import numpy as np
import pandas as pd
import pytest

from ml.recommendation.cf.CF import (
    build_cooccurrence_index,
    get_cf_scores,
    _score_by_cooccurrence,
    _score_by_embedding_fallback,
)

DIM = 386


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_songs_df(n: int = 10, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "video_id":    [f"vid_{i}" for i in range(n)],
        "track_title": [f"Track {i}" for i in range(n)],
        "artist_name": [f"Artist {i}" for i in range(n)],
        "genre":       ["pop" if i % 2 == 0 else "hiphoprap" for i in range(n)],
        "embedding":   [rng.random(DIM).astype(np.float32) for _ in range(n)],
    })


def _simple_population() -> dict:
    """
    Hand-crafted population for deterministic co-occurrence tests.

    User A: vid_0, vid_1, vid_2
    User B: vid_0, vid_1, vid_3
    User C: vid_1, vid_2, vid_4
    User D: vid_5, vid_6          (completely different taste)

    Co-occurrence counts:
        (vid_0, vid_1): 2   (users A and B)
        (vid_0, vid_2): 1   (user A)
        (vid_0, vid_3): 1   (user B)
        (vid_1, vid_2): 2   (users A and C)
        (vid_1, vid_3): 1   (user B)
        (vid_1, vid_4): 1   (user C)
        (vid_2, vid_4): 1   (user C)
        (vid_5, vid_6): 1   (user D)

    Popularity:
        vid_0: 2, vid_1: 3, vid_2: 2, vid_3: 1, vid_4: 1,
        vid_5: 1, vid_6: 1
    """
    return {
        "user_a": ["vid_0", "vid_1", "vid_2"],
        "user_b": ["vid_0", "vid_1", "vid_3"],
        "user_c": ["vid_1", "vid_2", "vid_4"],
        "user_d": ["vid_5", "vid_6"],
    }


# ── Tests: build_cooccurrence_index ──────────────────────────────────────────

class TestBuildCooccurrenceIndex:
    def test_pair_count_is_symmetric(self):
        pop = _simple_population()
        cooc, _ = build_cooccurrence_index(pop)
        assert cooc["vid_0"]["vid_1"] == cooc["vid_1"]["vid_0"]

    def test_known_pair_count(self):
        pop = _simple_population()
        cooc, _ = build_cooccurrence_index(pop)
        assert cooc["vid_0"]["vid_1"] == 2

    def test_popularity_counts(self):
        pop = _simple_population()
        _, popularity = build_cooccurrence_index(pop)
        assert popularity["vid_0"] == 2
        assert popularity["vid_1"] == 3
        assert popularity["vid_5"] == 1

    def test_no_self_cooccurrence(self):
        pop = _simple_population()
        cooc, _ = build_cooccurrence_index(pop)
        for vid, neighbors in cooc.items():
            assert vid not in neighbors, f"{vid} co-occurs with itself"

    def test_empty_population_returns_empty(self):
        cooc, pop = build_cooccurrence_index({})
        assert cooc == {}
        assert pop == {}

    def test_single_user_no_cooccurrence_with_others(self):
        pop = {"only_user": ["vid_x", "vid_y"]}
        cooc, popularity = build_cooccurrence_index(pop)
        assert cooc["vid_x"]["vid_y"] == 1
        assert popularity["vid_x"] == 1

    def test_duplicate_liked_songs_counted_once_per_user(self):
        pop = {"user_a": ["vid_0", "vid_0", "vid_1"]}
        cooc, popularity = build_cooccurrence_index(pop)
        assert cooc.get("vid_0", {}).get("vid_1", 0) == 1
        assert popularity["vid_0"] == 1


# ── Tests: _score_by_cooccurrence ─────────────────────────────────────────────

class TestScoreByCooccurrence:
    def setup_method(self):
        pop = _simple_population()
        self.cooc, self.pop = build_cooccurrence_index(pop)

    def test_songs_with_cooccurrence_have_nonzero_scores(self):
        group_liked = {"vid_0"}
        scores = _score_by_cooccurrence(
            ["vid_1", "vid_3"], group_liked,
            self.cooc, self.pop, min_cooccurrence=1
        )
        assert scores["vid_1"] > 0.0
        assert scores["vid_3"] > 0.0
        assert scores["vid_3"] > scores["vid_1"]

    def test_song_with_no_overlap_scores_zero(self):
        group_liked = {"vid_0"}
        scores = _score_by_cooccurrence(
            ["vid_5"], group_liked,
            self.cooc, self.pop, min_cooccurrence=1
        )
        assert scores["vid_5"] == 0.0

    def test_min_cooccurrence_threshold_filters_weak_signal(self):
        group_liked = {"vid_0"}
        scores_strict = _score_by_cooccurrence(
            ["vid_2"], group_liked,
            self.cooc, self.pop, min_cooccurrence=2
        )
        scores_loose = _score_by_cooccurrence(
            ["vid_2"], group_liked,
            self.cooc, self.pop, min_cooccurrence=1
        )
        assert scores_strict["vid_2"] == 0.0
        assert scores_loose["vid_2"] > 0.0

    def test_scores_capped_at_1(self):
        group_liked = {"vid_0", "vid_1", "vid_2"}
        scores = _score_by_cooccurrence(
            ["vid_4"], group_liked,
            self.cooc, self.pop, min_cooccurrence=1
        )
        assert scores["vid_4"] <= 1.0

    def test_unknown_candidate_scores_zero(self):
        group_liked = {"vid_0"}
        scores = _score_by_cooccurrence(
            ["vid_999"], group_liked,
            self.cooc, self.pop, min_cooccurrence=1
        )
        assert scores["vid_999"] == 0.0


# ── Tests: _score_by_embedding_fallback ──────────────────────────────────────

class TestEmbeddingFallback:
    def test_returns_scores_for_all_candidates(self):
        songs_df   = _make_songs_df(10)
        candidates = songs_df.head(6)
        group_liked = {"vid_7", "vid_8"}
        scores = _score_by_embedding_fallback(candidates, group_liked, songs_df)
        assert set(scores.keys()) == set(candidates["video_id"])

    def test_scores_are_in_0_to_1_range(self):
        songs_df   = _make_songs_df(10)
        candidates = songs_df
        group_liked = {"vid_0", "vid_1"}
        scores = _score_by_embedding_fallback(candidates, group_liked, songs_df)
        for vid, score in scores.items():
            assert 0.0 <= score <= 1.0, f"{vid} score {score} out of range"

    def test_empty_group_liked_returns_zeros(self):
        songs_df   = _make_songs_df(5)
        candidates = songs_df
        scores = _score_by_embedding_fallback(candidates, set(), songs_df)
        assert all(s == 0.0 for s in scores.values())


# ── Tests: get_cf_scores (full public API) ────────────────────────────────────

class TestGetCfScores:
    def test_returns_top_n_rows(self):
        songs_df = _make_songs_df(10)
        result = get_cf_scores(
            all_user_liked=_simple_population(),
            user_liked_songs={"user_1": ["vid_0", "vid_1"]},
            songs_df=songs_df,
            already_played_ids=set(),
            top_n=5,
        )
        assert len(result) == 5

    def test_output_columns_correct(self):
        songs_df = _make_songs_df(10)
        result = get_cf_scores(
            all_user_liked=_simple_population(),
            user_liked_songs={"user_1": ["vid_0"]},
            songs_df=songs_df,
            already_played_ids=set(),
            top_n=5,
        )
        expected = {"video_id", "track_title", "artist_name", "genre", "cf_score"}
        assert expected.issubset(set(result.columns))

    def test_excludes_already_played(self):
        songs_df = _make_songs_df(10)
        played   = {"vid_0", "vid_1", "vid_2"}
        result = get_cf_scores(
            all_user_liked=_simple_population(),
            user_liked_songs={"user_1": ["vid_3"]},
            songs_df=songs_df,
            already_played_ids=played,
            top_n=10,
        )
        assert not any(vid in played for vid in result["video_id"])

    def test_empty_population_triggers_fallback(self):
        songs_df = _make_songs_df(10)
        result = get_cf_scores(
            all_user_liked={},
            user_liked_songs={"user_1": ["vid_0"]},
            songs_df=songs_df,
            already_played_ids=set(),
            top_n=5,
        )
        assert len(result) == 5
        assert (result["cf_score"] >= 0.0).all()
        assert (result["cf_score"] <= 1.0).all()

    def test_no_group_liked_songs_returns_empty(self):
        songs_df = _make_songs_df(10)
        result = get_cf_scores(
            all_user_liked=_simple_population(),
            user_liked_songs={},
            songs_df=songs_df,
            already_played_ids=set(),
            top_n=5,
        )
        assert result.empty

    def test_sorted_by_cf_score_descending(self):
        songs_df = _make_songs_df(10)
        result = get_cf_scores(
            all_user_liked=_simple_population(),
            user_liked_songs={"user_1": ["vid_0", "vid_1"]},
            songs_df=songs_df,
            already_played_ids=set(),
            top_n=8,
        )
        scores = result["cf_score"].tolist()
        assert scores == sorted(scores, reverse=True)

    def test_all_zero_cooccurrence_triggers_fallback(self):
        songs_df = _make_songs_df(10)
        result = get_cf_scores(
            all_user_liked=_simple_population(),
            user_liked_songs={"user_1": ["vid_99", "vid_100"]},
            songs_df=songs_df,
            already_played_ids=set(),
            top_n=5,
        )
        assert len(result) == 5

    def test_empty_songs_df_returns_empty(self):
        result = get_cf_scores(
            all_user_liked=_simple_population(),
            user_liked_songs={"user_1": ["vid_0"]},
            songs_df=pd.DataFrame(),
            already_played_ids=set(),
            top_n=5,
        )
        assert result.empty

    def test_top_n_zero_returns_empty_not_crash(self):
        songs_df = _make_songs_df(10)
        result = get_cf_scores(
            all_user_liked=_simple_population(),
            user_liked_songs={"user_1": ["vid_0"]},
            songs_df=songs_df,
            already_played_ids=set(),
            top_n=0,
        )
        assert result.empty

    def test_all_songs_played_returns_empty(self):
        songs_df = _make_songs_df(5)
        played   = set(songs_df["video_id"].tolist())
        result = get_cf_scores(
            all_user_liked=_simple_population(),
            user_liked_songs={"user_1": ["vid_0"]},
            songs_df=songs_df,
            already_played_ids=played,
            top_n=5,
        )
        assert result.empty

    def test_max_prevents_multi_seed_double_counting(self):
        pop = {}
        for i in range(10): pop[f'ua{i}'] = ['song_A', 'seed_A']
        pop['ub0'] = ['song_A', 'seed_B']
        for i in range(5): pop[f'uc{i}'] = ['song_B', 'seed_A']
        for i in range(5): pop[f'ud{i}'] = ['song_B', 'seed_B']

        songs_df = pd.DataFrame({
            "video_id":    ["song_A", "song_B", "other"],
            "track_title": ["A", "B", "O"],
            "artist_name": ["X", "Y", "Z"],
            "genre":       ["pop"] * 3,
            "embedding":   [
                np.random.default_rng(i).random(DIM).astype(np.float32)
                for i in range(3)
            ],
        })

        result = get_cf_scores(
            all_user_liked=pop,
            user_liked_songs={"session_user": ["seed_A", "seed_B"]},
            songs_df=songs_df,
            already_played_ids=set(),
            top_n=3,
            min_cooccurrence=1,
        )
        scores = result.set_index("video_id")["cf_score"]
        assert scores["song_A"] > scores["song_B"]

    def test_normalisation_penalises_universally_popular_songs(self):
        group_song = "grp_0"
        popular    = "vid_popular"
        niche      = "vid_niche"

        pop: dict = {f"u_pop_{i}": [group_song, popular] for i in range(5)}
        pop.update({f"u_niche_{i}": [group_song, niche] for i in range(10)})
        pop.update({f"u_extra_{i}": [popular] for i in range(95)})

        songs_df = pd.DataFrame({
            "video_id":    [popular, niche, "other_0", "other_1"],
            "track_title": ["Popular Song", "Niche Song", "Other 0", "Other 1"],
            "artist_name": ["A", "B", "C", "D"],
            "genre":       ["pop"] * 4,
            "embedding":   [
                np.random.default_rng(i).random(DIM).astype(np.float32)
                for i in range(4)
            ],
        })

        result = get_cf_scores(
            all_user_liked=pop,
            user_liked_songs={"session_user": [group_song]},
            songs_df=songs_df,
            already_played_ids=set(),
            top_n=4,
            min_cooccurrence=1,
        )

        scores = result.set_index("video_id")["cf_score"]
        assert scores[niche] > scores[popular]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])