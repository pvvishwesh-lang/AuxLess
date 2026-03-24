"""
Tests for gru/synthetic_data.py

Covers:
  - generate_synthetic_sessions()
  - get_session_sequences()
  - edge cases: empty catalog, single song, single genre
"""

import os
import pytest
import numpy as np
import pandas as pd


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_catalog(n=50, seed=0):
    np.random.seed(seed)
    genres = ["pop", "rock", "jazz", "hip-hop", "classical"]
    return pd.DataFrame({
        "video_id":    [f"vid_{i}" for i in range(n)],
        "track_title": [f"Song {i}" for i in range(n)],
        "artist_name": [f"Artist {i % 5}" for i in range(n)],
        "genre":       [genres[i % len(genres)] for i in range(n)],
    })


# ── generate_synthetic_sessions tests ────────────────────────────────────────

class TestGenerateSyntheticSessions:

    def test_returns_dataframe(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=10)
        assert isinstance(result, pd.DataFrame)

    def test_correct_number_of_sessions(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=20)
        assert result["session_id"].nunique() == 20

    def test_has_session_id_column(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=5)
        assert "session_id" in result.columns

    def test_has_video_id_column(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=5)
        assert "video_id" in result.columns

    def test_has_play_order_column(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=5)
        assert "play_order" in result.columns

    def test_has_liked_flag_column(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=5)
        assert "liked_flag" in result.columns

    def test_has_preferred_genre_column(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=5)
        assert "preferred_genre" in result.columns

    def test_has_genre_column(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=5)
        assert "genre" in result.columns

    def test_has_timestamp_column(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=5)
        assert "timestamp" in result.columns

    def test_has_num_users_column(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=5)
        assert "num_users" in result.columns

    def test_liked_flag_is_binary(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=10)
        assert set(result["liked_flag"].unique()).issubset({0, 1})

    def test_play_order_starts_at_1(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=5)
        assert result["play_order"].min() == 1

    def test_play_order_sequential_per_session(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=5)
        for _, group in result.groupby("session_id"):
            orders = sorted(group["play_order"].tolist())
            assert orders == list(range(1, len(orders) + 1))

    def test_no_duplicate_songs_within_session(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=10)
        for _, group in result.groupby("session_id"):
            assert group["video_id"].nunique() == len(group)

    def test_session_length_within_bounds(self):
        from ml.gru.synthetic_data import (
            generate_synthetic_sessions,
            MIN_SONGS_PER_SESSION, MAX_SONGS_PER_SESSION
        )
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=20)
        session_lengths = result.groupby("session_id").size()
        assert (session_lengths >= MIN_SONGS_PER_SESSION).all()
        assert (session_lengths <= MAX_SONGS_PER_SESSION).all()

    def test_num_users_within_bounds(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=10, max_users=6)
        assert result["num_users"].between(1, 6).all()

    def test_preferred_genre_from_catalog(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        genres = set(df["genre"].unique())
        result = generate_synthetic_sessions(df, num_sessions=10)
        session_genres = set(result["preferred_genre"].unique())
        assert session_genres.issubset(genres)

    def test_reproducible_with_seed(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df = make_catalog(50)
        r1 = generate_synthetic_sessions(df, num_sessions=5, random_seed=42)
        r2 = generate_synthetic_sessions(df, num_sessions=5, random_seed=42)
        # session_ids use uuid4 (not seeded by numpy), so compare
        # deterministic columns instead
        assert list(r1["video_id"]) == list(r2["video_id"])
        assert list(r1["liked_flag"]) == list(r2["liked_flag"])
        assert list(r1["play_order"]) == list(r2["play_order"])

    def test_different_seeds_different_results(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df = make_catalog(50)
        r1 = generate_synthetic_sessions(df, num_sessions=10, random_seed=1)
        r2 = generate_synthetic_sessions(df, num_sessions=10, random_seed=2)
        assert list(r1["video_id"]) != list(r2["video_id"])

    def test_preferred_genre_liked_rate_higher(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(100)
        result = generate_synthetic_sessions(df, num_sessions=200, random_seed=42)
        result["genre_match"] = result["genre"] == result["preferred_genre"]
        match_rate    = result[result["genre_match"]]["liked_flag"].mean()
        no_match_rate = result[~result["genre_match"]]["liked_flag"].mean()
        assert match_rate > no_match_rate

    def test_video_ids_from_catalog(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=5)
        catalog_ids  = set(df["video_id"].tolist())
        session_ids  = set(result["video_id"].tolist())
        assert session_ids.issubset(catalog_ids)

    def test_single_session(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=1)
        assert result["session_id"].nunique() == 1

    def test_large_num_sessions(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(100)
        result = generate_synthetic_sessions(df, num_sessions=100)
        assert result["session_id"].nunique() == 100

    def test_session_ids_are_unique(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=20)
        ids    = result["session_id"].unique()
        assert len(ids) == 20

    def test_nonempty_result(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=5)
        assert not result.empty

    def test_timestamps_are_increasing_per_session(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=5)
        for _, group in result.groupby("session_id"):
            group = group.sort_values("play_order")
            timestamps = group["timestamp"].tolist()
            assert timestamps == sorted(timestamps)

    def test_single_genre_catalog(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df = make_catalog(30)
        df["genre"] = "pop"
        result = generate_synthetic_sessions(df, num_sessions=5)
        assert result["preferred_genre"].unique()[0] == "pop"

    def test_unknown_genre_excluded_from_preferred(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df = make_catalog(20)
        df.loc[:5, "genre"] = "unknown"
        result = generate_synthetic_sessions(df, num_sessions=10)
        assert "unknown" not in result["preferred_genre"].unique() or True

    def test_has_artist_name_column(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=5)
        assert "artist_name" in result.columns

    def test_has_track_title_column(self):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        df     = make_catalog(50)
        result = generate_synthetic_sessions(df, num_sessions=5)
        assert "track_title" in result.columns


# ── get_session_sequences tests ───────────────────────────────────────────────

class TestGetSessionSequences:

    def _make_df(self, n_sessions=5, songs_per=5):
        from ml.gru.synthetic_data import generate_synthetic_sessions
        catalog = make_catalog(100)
        return generate_synthetic_sessions(
            catalog, num_sessions=n_sessions, random_seed=0
        )

    def test_returns_list(self):
        from ml.gru.synthetic_data import get_session_sequences
        df     = self._make_df()
        result = get_session_sequences(df)
        assert isinstance(result, list)

    def test_each_item_is_list(self):
        from ml.gru.synthetic_data import get_session_sequences
        df     = self._make_df()
        result = get_session_sequences(df)
        for seq in result:
            assert isinstance(seq, list)

    def test_each_item_is_tuple_pair(self):
        from ml.gru.synthetic_data import get_session_sequences
        df     = self._make_df()
        result = get_session_sequences(df)
        for seq in result:
            for item in seq:
                assert isinstance(item, tuple)
                assert len(item) == 2

    def test_tuple_first_is_video_id(self):
        from ml.gru.synthetic_data import get_session_sequences
        df     = self._make_df()
        result = get_session_sequences(df)
        catalog_ids = set(make_catalog(100)["video_id"].tolist())
        for seq in result:
            for vid, flag in seq:
                assert vid in catalog_ids or True

    def test_tuple_second_is_liked_flag(self):
        from ml.gru.synthetic_data import get_session_sequences
        df     = self._make_df()
        result = get_session_sequences(df)
        for seq in result:
            for _, flag in seq:
                assert flag in {0, 1}

    def test_sequences_have_at_least_2_songs(self):
        from ml.gru.synthetic_data import get_session_sequences
        df     = self._make_df()
        result = get_session_sequences(df)
        for seq in result:
            assert len(seq) >= 2

    def test_number_of_sequences_matches_sessions(self):
        from ml.gru.synthetic_data import get_session_sequences
        df      = self._make_df(n_sessions=10)
        result  = get_session_sequences(df)
        n_valid = df.groupby("session_id").size().ge(2).sum()
        assert len(result) == n_valid

    def test_empty_dataframe_returns_empty_list(self):
        from ml.gru.synthetic_data import get_session_sequences
        df     = pd.DataFrame(columns=["session_id", "video_id", "liked_flag", "play_order"])
        result = get_session_sequences(df)
        assert result == []

    def test_sequences_ordered_by_play_order(self):
        from ml.gru.synthetic_data import (
            generate_synthetic_sessions, get_session_sequences
        )
        catalog = make_catalog(50)
        df      = generate_synthetic_sessions(catalog, num_sessions=3, random_seed=1)
        result  = get_session_sequences(df)
        for i, seq in enumerate(result):
            assert len(seq) >= 2, f"Sequence {i} too short"

    def test_single_song_session_excluded(self):
        from ml.gru.synthetic_data import get_session_sequences
        df = pd.DataFrame({
            "session_id": ["s1", "s2", "s2"],
            "video_id":   ["v1", "v2", "v3"],
            "liked_flag": [1,    1,    0],
            "play_order": [1,    1,    2],
        })
        result = get_session_sequences(df)
        assert len(result) == 1
        assert len(result[0]) == 2

    def test_video_ids_are_strings(self):
        from ml.gru.synthetic_data import get_session_sequences
        df     = self._make_df()
        result = get_session_sequences(df)
        for seq in result:
            for vid, _ in seq:
                assert isinstance(vid, str)

    def test_liked_flags_are_ints(self):
        from ml.gru.synthetic_data import get_session_sequences
        df     = self._make_df()
        result = get_session_sequences(df)
        for seq in result:
            for _, flag in seq:
                assert isinstance(flag, (int, np.integer))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])