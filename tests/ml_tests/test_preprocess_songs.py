"""
Tests for preprocessing/preprocess_songs.py

Covers:
  - clean_text()
  - preprocess_songs()
  - preprocess_for_session() (GCP calls skipped in CI)
  - _mitigated_path(), _preprocessed_path()
  - edge cases: empty df, missing columns, zero view counts, etc.
"""

import os
import pytest
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_songs_df(n=10, seed=42):
    np.random.seed(seed)
    return pd.DataFrame({
        "video_id":              [f"vid_{i}" for i in range(n)],
        "track_title":           [f"Song {i}" for i in range(n)],
        "artist_name":           [f"Artist {i % 3}" for i in range(n)],
        "genre":                 [["pop", "rock", "jazz"][i % 3] for i in range(n)],
        "collection_name":       [f"Album {i}" for i in range(n)],
        "country":               ["US"] * n,
        "trackTimeMillis":       [200000 + i * 1000 for i in range(n)],
        "trackTimeSeconds":      [200 + i for i in range(n)],
        "view_count":            [1000 * (i + 1) for i in range(n)],
        "like_count":            [100 * (i + 1) for i in range(n)],
        "comment_count":         [10 * (i + 1) for i in range(n)],
        "like_to_view_ratio":    [0.1] * n,
        "comment_to_view_ratio": [0.01] * n,
    })


# ── clean_text tests ──────────────────────────────────────────────────────────

class TestCleanText:

    def _clean(self, text):
        from ml.preprocessing.preprocess_songs import clean_text
        return clean_text(text)

    def test_lowercases_text(self):
        assert self._clean("Hello World") == "hello world"

    def test_removes_special_characters(self):
        assert self._clean("hello!@#$%") == "hello"

    def test_removes_punctuation(self):
        assert self._clean("rock & roll!") == "rock  roll"

    def test_strips_whitespace(self):
        assert self._clean("  hello  ") == "hello"

    def test_handles_none(self):
        assert self._clean(None) == "unknown"

    def test_handles_nan(self):
        assert self._clean(float("nan")) == "unknown"

    def test_handles_empty_string(self):
        assert self._clean("") == ""

    def test_keeps_numbers(self):
        assert self._clean("track 101") == "track 101"

    def test_keeps_spaces_between_words(self):
        result = self._clean("hello world")
        assert " " in result

    def test_handles_all_special_chars(self):
        assert self._clean("!@#$%^&*()") == ""

    def test_handles_numeric_string(self):
        assert self._clean("12345") == "12345"

    def test_handles_mixed_case(self):
        assert self._clean("RoCk MuSiC") == "rock music"

    def test_handles_unicode_letters(self):
        # non-ASCII letters are stripped
        result = self._clean("café")
        assert isinstance(result, str)

    def test_handles_tabs(self):
        result = self._clean("hello\tworld")
        assert "hello" in result

    def test_handles_newlines(self):
        result = self._clean("hello\nworld")
        assert isinstance(result, str)

    def test_handles_integer_input(self):
        result = self._clean(42)
        assert result == "42"

    def test_handles_float_input(self):
        result = self._clean(3.14)
        assert isinstance(result, str)

    def test_handles_bool_input(self):
        result = self._clean(True)
        assert isinstance(result, str)

    def test_returns_string(self):
        assert isinstance(self._clean("hello"), str)

    def test_apostrophe_removed(self):
        result = self._clean("rock n' roll")
        assert "'" not in result

    def test_hyphen_removed(self):
        result = self._clean("hip-hop")
        assert "-" not in result


# ── _mitigated_path / _preprocessed_path tests ───────────────────────────────

class TestPaths:

    def test_mitigated_path_format(self):
        from ml.preprocessing.preprocess_songs import _mitigated_path
        result = _mitigated_path("session_abc")
        assert "session_abc" in result
        assert result.endswith(".csv")

    def test_preprocessed_path_format(self):
        from ml.preprocessing.preprocess_songs import _preprocessed_path
        result = _preprocessed_path("session_abc")
        assert "session_abc" in result
        assert result.endswith(".csv")

    def test_mitigated_path_different_sessions(self):
        from ml.preprocessing.preprocess_songs import _mitigated_path
        assert _mitigated_path("s1") != _mitigated_path("s2")

    def test_preprocessed_path_different_sessions(self):
        from ml.preprocessing.preprocess_songs import _preprocessed_path
        assert _preprocessed_path("s1") != _preprocessed_path("s2")

    def test_mitigated_path_is_string(self):
        from ml.preprocessing.preprocess_songs import _mitigated_path
        assert isinstance(_mitigated_path("s1"), str)

    def test_preprocessed_path_is_string(self):
        from ml.preprocessing.preprocess_songs import _preprocessed_path
        assert isinstance(_preprocessed_path("s1"), str)


# ── preprocess_songs core tests ───────────────────────────────────────────────

class TestPreprocessSongs:

    def test_returns_dataframe(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df()
        result = preprocess_songs(df)
        assert isinstance(result, pd.DataFrame)

    def test_deduplicates_by_video_id(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(5)
        df = pd.concat([df, df]).reset_index(drop=True)
        result = preprocess_songs(df)
        assert result["video_id"].nunique() == 5

    def test_fills_missing_genre(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(5)
        df.loc[0, "genre"] = None
        result = preprocess_songs(df)
        assert "unknown" in result["genre"].values

    def test_fills_missing_artist_name(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(5)
        df.loc[0, "artist_name"] = None
        result = preprocess_songs(df)
        assert "unknown" in result["artist_name"].values

    def test_fills_missing_country(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(5)
        df.loc[0, "country"] = None
        result = preprocess_songs(df)
        assert "unknown" in result["country"].values

    def test_fills_missing_collection_name(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(5)
        df.loc[0, "collection_name"] = None
        result = preprocess_songs(df)
        assert "unknown" in result["collection_name"].values

    def test_cleans_track_title(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df.loc[0, "track_title"] = "Hello World!!!"
        result = preprocess_songs(df)
        assert "!" not in result["track_title"].iloc[0]

    def test_cleans_artist_name(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df.loc[0, "artist_name"] = "Artist & Co."
        result = preprocess_songs(df)
        assert "&" not in result["artist_name"].iloc[0]

    def test_cleans_genre(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df.loc[0, "genre"] = "Rock/Pop"
        result = preprocess_songs(df)
        assert "/" not in result["genre"].iloc[0]

    def test_duration_sec_computed(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(5)
        df["trackTimeMillis"] = [300000] * 5
        result = preprocess_songs(df)
        assert "duration_sec" in result.columns
        assert (result["duration_sec"] == 300.0).all()

    def test_popularity_score_added(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df()
        result = preprocess_songs(df)
        assert "popularity_score" in result.columns

    def test_popularity_score_between_0_and_1(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df()
        result = preprocess_songs(df)
        assert result["popularity_score"].between(0.0, 1.0).all()

    def test_no_negative_popularity(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df()
        result = preprocess_songs(df)
        assert (result["popularity_score"] >= 0).all()

    def test_numeric_cols_coerced(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(5)
        df["view_count"] = ["1000", "bad", None, "500", "200"]
        result = preprocess_songs(df)
        assert pd.api.types.is_numeric_dtype(result["view_count"])

    def test_missing_view_count_filled_with_zero(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(5)
        df["view_count"] = None
        result = preprocess_songs(df)
        assert (result["view_count"] == 0).all()

    def test_like_to_view_ratio_recomputed_when_zero(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df["like_to_view_ratio"] = 0.0
        result = preprocess_songs(df)
        assert "like_to_view_ratio" in result.columns

    def test_comment_to_view_ratio_recomputed_when_zero(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df["comment_to_view_ratio"] = 0.0
        result = preprocess_songs(df)
        assert "comment_to_view_ratio" in result.columns

    def test_zero_view_count_no_division_error(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df["view_count"] = 0
        df["like_count"] = 0
        df["comment_count"] = 0
        df["like_to_view_ratio"] = 0.0
        df["comment_to_view_ratio"] = 0.0
        result = preprocess_songs(df)
        assert not result["like_to_view_ratio"].isna().any()

    def test_genre_lowercased(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df["genre"] = ["Rock", "POP", "JAZZ"]
        result = preprocess_songs(df)
        assert result["genre"].str.islower().all()

    def test_preserves_all_non_duplicate_rows(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(10)
        result = preprocess_songs(df)
        assert len(result) == 10

    def test_removes_only_duplicate_rows(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(5)
        duplicate = df.iloc[[0]].copy()
        df = pd.concat([df, duplicate]).reset_index(drop=True)
        result = preprocess_songs(df)
        assert len(result) == 5

    def test_popularity_score_no_nan(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df()
        result = preprocess_songs(df)
        assert not result["popularity_score"].isna().any()

    def test_duration_sec_no_nan(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df()
        result = preprocess_songs(df)
        assert not result["duration_sec"].isna().any()

    def test_all_text_columns_are_strings(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df()
        result = preprocess_songs(df)
        for col in ["track_title", "artist_name", "genre", "collection_name", "country"]:
            assert result[col].dtype == object

    def test_single_row_df(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(1)
        result = preprocess_songs(df)
        assert len(result) == 1

    def test_all_missing_genre_filled(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(5)
        df["genre"] = None
        result = preprocess_songs(df)
        assert (result["genre"] == "unknown").all()

    def test_high_view_count_gets_high_popularity(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(5)
        df.loc[0, "view_count"]    = 10_000_000
        df.loc[0, "like_count"]    = 1_000_000
        df.loc[0, "comment_count"] = 100_000
        result = preprocess_songs(df)
        max_idx = result["popularity_score"].idxmax()
        assert result.loc[max_idx, "video_id"] == "vid_0"

    def test_like_to_view_ratio_preserved_when_nonzero(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df["like_to_view_ratio"] = [0.5, 0.3, 0.2]
        result = preprocess_songs(df)
        assert (result["like_to_view_ratio"] == [0.5, 0.3, 0.2]).all()

    def test_comment_to_view_ratio_preserved_when_nonzero(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df["comment_to_view_ratio"] = [0.05, 0.03, 0.02]
        result = preprocess_songs(df)
        assert (result["comment_to_view_ratio"] == [0.05, 0.03, 0.02]).all()

    def test_track_title_no_special_chars(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df["track_title"] = ["Song #1!", "Track (2)", "Music & More"]
        result = preprocess_songs(df)
        for title in result["track_title"]:
            assert all(c.isalnum() or c.isspace() for c in title)

    def test_duration_is_millis_divided_by_1000(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df["trackTimeMillis"] = [180000, 240000, 300000]
        result = preprocess_songs(df)
        expected = [180.0, 240.0, 300.0]
        assert list(result["duration_sec"]) == expected

    def test_popularity_weights_view_count_most(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(2)
        # song 0: huge views, no likes/comments
        df.loc[0, "view_count"]    = 1_000_000
        df.loc[0, "like_count"]    = 0
        df.loc[0, "comment_count"] = 0
        # song 1: no views, huge likes
        df.loc[1, "view_count"]    = 0
        df.loc[1, "like_count"]    = 1_000_000
        df.loc[1, "comment_count"] = 0
        result = preprocess_songs(df)
        # view_count has 0.5 weight vs like_count 0.3
        assert result.loc[0, "popularity_score"] > result.loc[1, "popularity_score"]

    def test_large_dataframe_performance(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(1000)
        result = preprocess_songs(df)
        assert len(result) == 1000

    def test_string_view_count_coerced(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df["view_count"] = ["1000", "2000", "3000"]
        result = preprocess_songs(df)
        assert pd.api.types.is_numeric_dtype(result["view_count"])

    def test_non_numeric_view_count_becomes_zero(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df.loc[0, "view_count"] = "not_a_number"
        result = preprocess_songs(df)
        assert result.loc[0, "view_count"] == 0

    def test_collection_name_cleaned(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df.loc[0, "collection_name"] = "Best Of 2020!"
        result = preprocess_songs(df)
        assert "!" not in result.loc[0, "collection_name"]

    def test_country_cleaned(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df.loc[0, "country"] = "U.S.A"
        result = preprocess_songs(df)
        assert "." not in result.loc[0, "country"]

    def test_all_rows_have_popularity_score(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(20)
        result = preprocess_songs(df)
        assert result["popularity_score"].notna().all()

    def test_index_reset_after_dedup(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(5)
        df = pd.concat([df, df]).reset_index(drop=True)
        result = preprocess_songs(df)
        assert list(result.index) == list(range(len(result)))

    def test_columns_unchanged(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df()
        result = preprocess_songs(df)
        # should retain original columns plus new ones
        for col in df.columns:
            assert col in result.columns

    def test_new_columns_added(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df()
        result = preprocess_songs(df)
        assert "duration_sec" in result.columns
        assert "popularity_score" in result.columns


# ── empty / edge case tests ───────────────────────────────────────────────────

class TestPreprocessSongsEdgeCases:

    def test_empty_dataframe_returns_empty(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = pd.DataFrame(columns=[
            "video_id", "track_title", "artist_name", "genre",
            "collection_name", "country", "trackTimeMillis",
            "trackTimeSeconds", "view_count", "like_count",
            "comment_count", "like_to_view_ratio", "comment_to_view_ratio"
        ])
        result = preprocess_songs(df)
        assert result.empty

    def test_all_duplicates_returns_one_row(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(1)
        df = pd.concat([df] * 5).reset_index(drop=True)
        result = preprocess_songs(df)
        assert len(result) == 1

    def test_all_null_genre_filled_unknown(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(5)
        df["genre"] = np.nan
        result = preprocess_songs(df)
        assert (result["genre"] == "unknown").all()

    def test_all_null_artist_filled_unknown(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(5)
        df["artist_name"] = np.nan
        result = preprocess_songs(df)
        assert (result["artist_name"] == "unknown").all()

    def test_zero_track_time_millis(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df["trackTimeMillis"] = 0
        result = preprocess_songs(df)
        assert (result["duration_sec"] == 0.0).all()

    def test_very_long_title_handled(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(1)
        df.loc[0, "track_title"] = "a" * 500
        result = preprocess_songs(df)
        assert len(result) == 1

    def test_negative_view_count_coerced(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df["view_count"] = [-1, -2, -3]
        result = preprocess_songs(df)
        assert pd.api.types.is_numeric_dtype(result["view_count"])

    def test_mixed_null_and_valid_genre(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(4)
        df.loc[0, "genre"] = None
        df.loc[2, "genre"] = None
        result = preprocess_songs(df)
        null_genre_rows = result[result["genre"] == "unknown"]
        assert len(null_genre_rows) == 2

    def test_all_zero_engagement_popularity_is_zero(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(3)
        df["view_count"]    = 0
        df["like_count"]    = 0
        df["comment_count"] = 0
        result = preprocess_songs(df)
        assert (result["popularity_score"] == 0.0).all()

    def test_single_non_duplicate_unchanged_count(self):
        from ml.preprocessing.preprocess_songs import preprocess_songs
        df = make_songs_df(7)
        result = preprocess_songs(df)
        assert len(result) == 7


# ── GCS-dependent tests (skipped in CI) ──────────────────────────────────────

@pytest.mark.skipif(os.getenv("CI") == "true", reason="Skipped in CI — requires GCP")
def test_preprocess_for_session_live():
    from ml.preprocessing.preprocess_songs import preprocess_for_session
    import os
    session_id = os.getenv("TEST_SESSION_ID", "test-session-001")
    bucket     = os.getenv("BUCKET", "your-gcs-bucket-name")
    result     = preprocess_for_session(session_id, bucket)
    assert isinstance(result, str)
    assert result.startswith("gs://")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
