"""
Tests for feature_engineering/generate_song_embeddings.py

Covers:
  - build_song_text()
  - generate_embeddings()
  - build_embedding_dataframe()
  - generate_embeddings_for_session() (GCP skipped in CI)
"""

import os
import pytest
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_songs_df(n=5, seed=0):
    np.random.seed(seed)
    genres = ["pop", "rock", "jazz", "hip-hop", "classical"]
    return pd.DataFrame({
        "video_id":         [f"vid_{i}" for i in range(n)],
        "track_title":      [f"song {i}" for i in range(n)],
        "artist_name":      [f"artist {i % 3}" for i in range(n)],
        "genre":            [genres[i % len(genres)] for i in range(n)],
        "collection_name":  [f"album {i}" for i in range(n)],
        "duration_sec":     [200.0 + i * 10 for i in range(n)],
        "popularity_score": [round(0.1 * i, 2) for i in range(n)],
    })


# ── build_song_text tests ─────────────────────────────────────────────────────

class TestBuildSongText:

    def _text(self, row):
        from ml.feature_engineering.generate_song_embeddings import build_song_text
        return build_song_text(row)

    def test_includes_title(self):
        row = {"track_title": "my song", "artist_name": "artist", "genre": "pop", "collection_name": "album"}
        assert "my song" in self._text(row)

    def test_includes_artist(self):
        row = {"track_title": "song", "artist_name": "my artist", "genre": "rock", "collection_name": "album"}
        assert "my artist" in self._text(row)

    def test_includes_genre(self):
        row = {"track_title": "song", "artist_name": "artist", "genre": "jazz", "collection_name": "album"}
        assert "jazz" in self._text(row)

    def test_includes_album(self):
        row = {"track_title": "song", "artist_name": "artist", "genre": "pop", "collection_name": "my album"}
        assert "my album" in self._text(row)

    def test_returns_string(self):
        row = {"track_title": "song", "artist_name": "artist", "genre": "pop", "collection_name": "album"}
        assert isinstance(self._text(row), str)

    def test_contains_title_prefix(self):
        row = {"track_title": "song", "artist_name": "artist", "genre": "pop", "collection_name": "album"}
        assert "title:" in self._text(row)

    def test_contains_artist_prefix(self):
        row = {"track_title": "song", "artist_name": "artist", "genre": "pop", "collection_name": "album"}
        assert "artist:" in self._text(row)

    def test_contains_genre_prefix(self):
        row = {"track_title": "song", "artist_name": "artist", "genre": "pop", "collection_name": "album"}
        assert "genre:" in self._text(row)

    def test_contains_album_prefix(self):
        row = {"track_title": "song", "artist_name": "artist", "genre": "pop", "collection_name": "album"}
        assert "album:" in self._text(row)

    def test_nonempty_result(self):
        row = {"track_title": "s", "artist_name": "a", "genre": "g", "collection_name": "c"}
        assert len(self._text(row)) > 0

    def test_different_songs_different_text(self):
        row1 = {"track_title": "song a", "artist_name": "artist1", "genre": "pop", "collection_name": "album1"}
        row2 = {"track_title": "song b", "artist_name": "artist2", "genre": "rock", "collection_name": "album2"}
        assert self._text(row1) != self._text(row2)

    def test_same_song_same_text(self):
        row = {"track_title": "song", "artist_name": "artist", "genre": "pop", "collection_name": "album"}
        assert self._text(row) == self._text(row)

    def test_handles_unknown_values(self):
        row = {"track_title": "unknown", "artist_name": "unknown", "genre": "unknown", "collection_name": "unknown"}
        result = self._text(row)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_order_is_consistent(self):
        row = {"track_title": "t", "artist_name": "a", "genre": "g", "collection_name": "c"}
        result = self._text(row)
        title_pos  = result.index("title:")
        artist_pos = result.index("artist:")
        genre_pos  = result.index("genre:")
        album_pos  = result.index("album:")
        assert title_pos < artist_pos < genre_pos < album_pos


# ── generate_embeddings tests ─────────────────────────────────────────────────

class TestGenerateEmbeddings:

    def test_returns_ndarray(self):
        from ml.feature_engineering.generate_song_embeddings import generate_embeddings
        df     = make_songs_df(3)
        result = generate_embeddings(df)
        assert isinstance(result, np.ndarray)

    def test_output_shape_rows(self):
        from ml.feature_engineering.generate_song_embeddings import generate_embeddings
        df     = make_songs_df(5)
        result = generate_embeddings(df)
        assert result.shape[0] == 5

    def test_output_shape_cols_386(self):
        from ml.feature_engineering.generate_song_embeddings import generate_embeddings
        df     = make_songs_df(3)
        result = generate_embeddings(df)
        assert result.shape[1] == 386

    def test_384_text_dims_plus_2_numeric(self):
        from ml.feature_engineering.generate_song_embeddings import generate_embeddings
        df     = make_songs_df(2)
        result = generate_embeddings(df)
        assert result.shape[1] == 386   # 384 + 2

    def test_no_nan_in_embeddings(self):
        from ml.feature_engineering.generate_song_embeddings import generate_embeddings
        df     = make_songs_df(3)
        result = generate_embeddings(df)
        assert not np.isnan(result).any()

    def test_no_inf_in_embeddings(self):
        from ml.feature_engineering.generate_song_embeddings import generate_embeddings
        df     = make_songs_df(3)
        result = generate_embeddings(df)
        assert not np.isinf(result).any()

    def test_numeric_features_in_0_1_range(self):
        from ml.feature_engineering.generate_song_embeddings import generate_embeddings
        df     = make_songs_df(5)
        result = generate_embeddings(df)
        # last 2 dims are MinMaxScaled -> [0, 1]
        numeric_part = result[:, 384:]
        assert (numeric_part >= 0.0).all()
        assert (numeric_part <= 1.0).all()

    def test_single_song(self):
        from ml.feature_engineering.generate_song_embeddings import generate_embeddings
        df     = make_songs_df(1)
        result = generate_embeddings(df)
        assert result.shape == (1, 386)

    def test_different_songs_different_embeddings(self):
        from ml.feature_engineering.generate_song_embeddings import generate_embeddings
        df     = make_songs_df(3)
        result = generate_embeddings(df)
        assert not np.allclose(result[0], result[1])

    def test_same_song_same_embedding(self):
        from ml.feature_engineering.generate_song_embeddings import generate_embeddings
        df = make_songs_df(1)
        df = pd.concat([df, df]).reset_index(drop=True)
        result = generate_embeddings(df)
        assert np.allclose(result[0], result[1], atol=1e-5)

    def test_song_text_column_added(self):
        from ml.feature_engineering.generate_song_embeddings import generate_embeddings
        df     = make_songs_df(3)
        generate_embeddings(df)
        assert "song_text" in df.columns

    def test_large_batch(self):
        from ml.feature_engineering.generate_song_embeddings import generate_embeddings
        df     = make_songs_df(20)
        result = generate_embeddings(df)
        assert result.shape == (20, 386)

    def test_text_embeddings_are_384_dims(self):
        from ml.feature_engineering.generate_song_embeddings import generate_embeddings
        df     = make_songs_df(2)
        result = generate_embeddings(df)
        text_part = result[:, :384]
        assert text_part.shape[1] == 384

    def test_float32_dtype(self):
        from ml.feature_engineering.generate_song_embeddings import generate_embeddings
        df     = make_songs_df(2)
        result = generate_embeddings(df)
        assert result.dtype in [np.float32, np.float64]


# ── build_embedding_dataframe tests ──────────────────────────────────────────

class TestBuildEmbeddingDataframe:

    def _build(self, n=5):
        from ml.feature_engineering.generate_song_embeddings import (
            generate_embeddings, build_embedding_dataframe
        )
        df         = make_songs_df(n)
        embeddings = generate_embeddings(df)
        return build_embedding_dataframe(df, embeddings)

    def test_returns_dataframe(self):
        result = self._build()
        assert isinstance(result, pd.DataFrame)

    def test_has_video_id_column(self):
        assert "video_id" in self._build().columns

    def test_has_track_title_column(self):
        assert "track_title" in self._build().columns

    def test_has_artist_name_column(self):
        assert "artist_name" in self._build().columns

    def test_has_genre_column(self):
        assert "genre" in self._build().columns

    def test_has_duration_sec_column(self):
        assert "duration_sec" in self._build().columns

    def test_has_popularity_score_column(self):
        assert "popularity_score" in self._build().columns

    def test_has_embedding_column(self):
        assert "embedding" in self._build().columns

    def test_correct_row_count(self):
        result = self._build(5)
        assert len(result) == 5

    def test_embedding_is_list(self):
        result = self._build(3)
        assert isinstance(result["embedding"].iloc[0], list)

    def test_embedding_length_386(self):
        result = self._build(3)
        assert len(result["embedding"].iloc[0]) == 386

    def test_video_ids_preserved(self):
        from ml.feature_engineering.generate_song_embeddings import (
            generate_embeddings, build_embedding_dataframe
        )
        df         = make_songs_df(5)
        embeddings = generate_embeddings(df)
        result     = build_embedding_dataframe(df, embeddings)
        assert list(result["video_id"]) == list(df["video_id"])

    def test_index_reset(self):
        from ml.feature_engineering.generate_song_embeddings import (
            generate_embeddings, build_embedding_dataframe
        )
        df         = make_songs_df(5)
        embeddings = generate_embeddings(df)
        result     = build_embedding_dataframe(df, embeddings)
        assert list(result.index) == list(range(5))

    def test_no_null_video_ids(self):
        result = self._build(5)
        assert result["video_id"].notna().all()

    def test_no_null_embeddings(self):
        result = self._build(3)
        assert result["embedding"].notna().all()

    def test_single_row(self):
        result = self._build(1)
        assert len(result) == 1
        assert len(result["embedding"].iloc[0]) == 386

    def test_embedding_values_are_floats(self):
        result = self._build(2)
        emb = result["embedding"].iloc[0]
        assert all(isinstance(v, float) for v in emb)

    def test_popularity_score_preserved(self):
        from ml.feature_engineering.generate_song_embeddings import (
            generate_embeddings, build_embedding_dataframe
        )
        df         = make_songs_df(3)
        embeddings = generate_embeddings(df)
        result     = build_embedding_dataframe(df, embeddings)
        assert list(result["popularity_score"]) == list(df["popularity_score"])


# ── generate_embeddings_for_session tests (GCP skipped in CI) ────────────────

class TestGenerateEmbeddingsForSession:

    @patch("ml.feature_engineering.generate_song_embeddings._read_csv_from_gcs")
    @patch("ml.feature_engineering.generate_song_embeddings.get_client")
    @patch("ml.feature_engineering.generate_song_embeddings._get_existing_ids")
    @patch("ml.feature_engineering.generate_song_embeddings.load_embeddings")
    def test_returns_int(
        self, mock_load, mock_existing, mock_client, mock_read
    ):
        from ml.feature_engineering.generate_song_embeddings import (
            generate_embeddings_for_session
        )
        mock_read.return_value     = make_songs_df(5)
        mock_existing.return_value = set()
        result = generate_embeddings_for_session("session_1", "my-bucket")
        assert isinstance(result, int)

    @patch("ml.feature_engineering.generate_song_embeddings._read_csv_from_gcs")
    @patch("ml.feature_engineering.generate_song_embeddings.get_client")
    @patch("ml.feature_engineering.generate_song_embeddings._get_existing_ids")
    @patch("ml.feature_engineering.generate_song_embeddings.load_embeddings")
    def test_returns_zero_on_empty_df(
        self, mock_load, mock_existing, mock_client, mock_read
    ):
        from ml.feature_engineering.generate_song_embeddings import (
            generate_embeddings_for_session
        )
        mock_read.return_value = pd.DataFrame()
        result = generate_embeddings_for_session("session_1", "my-bucket")
        assert result == 0

    @patch("ml.feature_engineering.generate_song_embeddings._read_csv_from_gcs")
    @patch("ml.feature_engineering.generate_song_embeddings.get_client")
    @patch("ml.feature_engineering.generate_song_embeddings._get_existing_ids")
    @patch("ml.feature_engineering.generate_song_embeddings.load_embeddings")
    def test_returns_zero_when_all_existing(
        self, mock_load, mock_existing, mock_client, mock_read
    ):
        from ml.feature_engineering.generate_song_embeddings import (
            generate_embeddings_for_session
        )
        df = make_songs_df(3)
        mock_read.return_value     = df
        mock_existing.return_value = set(df["video_id"].tolist())
        result = generate_embeddings_for_session("session_1", "my-bucket")
        assert result == 0

    @patch("ml.feature_engineering.generate_song_embeddings._read_csv_from_gcs")
    @patch("ml.feature_engineering.generate_song_embeddings.get_client")
    @patch("ml.feature_engineering.generate_song_embeddings._get_existing_ids")
    @patch("ml.feature_engineering.generate_song_embeddings.load_embeddings")
    def test_load_embeddings_called_for_new_songs(
        self, mock_load, mock_existing, mock_client, mock_read
    ):
        from ml.feature_engineering.generate_song_embeddings import (
            generate_embeddings_for_session
        )
        mock_read.return_value     = make_songs_df(5)
        mock_existing.return_value = set()
        generate_embeddings_for_session("session_1", "my-bucket")
        mock_load.assert_called_once()

    @patch("ml.feature_engineering.generate_song_embeddings._read_csv_from_gcs")
    @patch("ml.feature_engineering.generate_song_embeddings.get_client")
    @patch("ml.feature_engineering.generate_song_embeddings._get_existing_ids")
    @patch("ml.feature_engineering.generate_song_embeddings.load_embeddings")
    def test_load_embeddings_not_called_when_all_existing(
        self, mock_load, mock_existing, mock_client, mock_read
    ):
        from ml.feature_engineering.generate_song_embeddings import (
            generate_embeddings_for_session
        )
        df = make_songs_df(3)
        mock_read.return_value     = df
        mock_existing.return_value = set(df["video_id"].tolist())
        generate_embeddings_for_session("session_1", "my-bucket")
        mock_load.assert_not_called()

    @patch("ml.feature_engineering.generate_song_embeddings._read_csv_from_gcs")
    @patch("ml.feature_engineering.generate_song_embeddings.get_client")
    @patch("ml.feature_engineering.generate_song_embeddings._get_existing_ids")
    @patch("ml.feature_engineering.generate_song_embeddings.load_embeddings")
    def test_returns_count_of_new_songs(
        self, mock_load, mock_existing, mock_client, mock_read
    ):
        from ml.feature_engineering.generate_song_embeddings import (
            generate_embeddings_for_session
        )
        mock_read.return_value     = make_songs_df(5)
        mock_existing.return_value = {"vid_0", "vid_1"}
        result = generate_embeddings_for_session("session_1", "my-bucket")
        assert result == 3


@pytest.mark.skipif(os.getenv("CI") == "true", reason="Skipped in CI — requires GCP")
def test_generate_embeddings_for_session_live():
    from ml.feature_engineering.generate_song_embeddings import (
        generate_embeddings_for_session
    )
    session_id = os.getenv("TEST_SESSION_ID", "test-session-001")
    bucket     = os.getenv("BUCKET", "your-gcs-bucket-name")
    result     = generate_embeddings_for_session(session_id, bucket)
    assert isinstance(result, int)
    assert result >= 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])