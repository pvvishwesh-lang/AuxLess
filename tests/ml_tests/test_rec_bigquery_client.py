"""
Tests for recommendation/bigquery_client.py

Covers:
  - get_client()
  - fetch_all_embeddings()
  - get_existing_video_ids()
  - load_embeddings()
  - get_user_liked_songs()
  - get_user_disliked_songs()
"""

import os
import pytest
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_bq_row(video_id, track_title="t", artist_name="a", genre="pop",
                embedding=None):
    row = MagicMock()
    row.video_id    = video_id
    row.track_title = track_title
    row.artist_name = artist_name
    row.genre       = genre
    row.embedding   = embedding or list(np.random.rand(386).astype(np.float32))
    return row


def make_user_row(user_id, liked=None, disliked=None):
    row = MagicMock()
    row.user_id       = user_id
    row.liked_songs   = liked   or []
    row.disliked_songs = disliked or []
    return row


def make_mock_client(query_rows=None, insert_errors=None):
    client = MagicMock()
    client.query.return_value.result.return_value = query_rows or []
    client.insert_rows_json.return_value           = insert_errors or []
    return client


def make_embedding_df(n=5):
    np.random.seed(0)
    return pd.DataFrame({
        "video_id":         [f"vid_{i}" for i in range(n)],
        "track_title":      [f"Song {i}" for i in range(n)],
        "artist_name":      [f"Artist {i}" for i in range(n)],
        "genre":            ["pop"] * n,
        "duration_sec":     [200.0] * n,
        "popularity_score": [0.5] * n,
        "embedding":        [list(np.random.rand(386)) for _ in range(n)],
    })


# ── get_client tests ──────────────────────────────────────────────────────────

class TestGetClient:

    @patch("ml.recommendation.bigquery_client.bigquery.Client")
    def test_returns_client(self, mock_bq):
        from ml.recommendation.bigquery_client import get_client
        mock_bq.return_value = MagicMock()
        assert get_client() is not None

    @patch("ml.recommendation.bigquery_client.bigquery.Client")
    def test_uses_project_id(self, mock_bq):
        from ml.recommendation.bigquery_client import get_client, PROJECT_ID
        get_client()
        mock_bq.assert_called_once_with(project=PROJECT_ID)


# ── fetch_all_embeddings tests ────────────────────────────────────────────────

class TestFetchAllEmbeddings:

    def test_returns_dataframe(self):
        from ml.recommendation.bigquery_client import fetch_all_embeddings
        rows   = [make_bq_row(f"vid_{i}") for i in range(5)]
        client = make_mock_client(query_rows=rows)
        result = fetch_all_embeddings(client)
        assert isinstance(result, pd.DataFrame)

    def test_correct_row_count(self):
        from ml.recommendation.bigquery_client import fetch_all_embeddings
        rows   = [make_bq_row(f"vid_{i}") for i in range(7)]
        client = make_mock_client(query_rows=rows)
        result = fetch_all_embeddings(client)
        assert len(result) == 7

    def test_has_video_id_column(self):
        from ml.recommendation.bigquery_client import fetch_all_embeddings
        client = make_mock_client(query_rows=[make_bq_row("vid_0")])
        result = fetch_all_embeddings(client)
        assert "video_id" in result.columns

    def test_has_embedding_column(self):
        from ml.recommendation.bigquery_client import fetch_all_embeddings
        client = make_mock_client(query_rows=[make_bq_row("vid_0")])
        result = fetch_all_embeddings(client)
        assert "embedding" in result.columns

    def test_has_track_title_column(self):
        from ml.recommendation.bigquery_client import fetch_all_embeddings
        client = make_mock_client(query_rows=[make_bq_row("vid_0")])
        result = fetch_all_embeddings(client)
        assert "track_title" in result.columns

    def test_has_artist_name_column(self):
        from ml.recommendation.bigquery_client import fetch_all_embeddings
        client = make_mock_client(query_rows=[make_bq_row("vid_0")])
        result = fetch_all_embeddings(client)
        assert "artist_name" in result.columns

    def test_has_genre_column(self):
        from ml.recommendation.bigquery_client import fetch_all_embeddings
        client = make_mock_client(query_rows=[make_bq_row("vid_0")])
        result = fetch_all_embeddings(client)
        assert "genre" in result.columns

    def test_embedding_is_numpy_array(self):
        from ml.recommendation.bigquery_client import fetch_all_embeddings
        client = make_mock_client(query_rows=[make_bq_row("vid_0")])
        result = fetch_all_embeddings(client)
        assert isinstance(result["embedding"].iloc[0], np.ndarray)

    def test_embedding_dtype_float32(self):
        from ml.recommendation.bigquery_client import fetch_all_embeddings
        client = make_mock_client(query_rows=[make_bq_row("vid_0")])
        result = fetch_all_embeddings(client)
        assert result["embedding"].iloc[0].dtype == np.float32

    def test_embedding_shape_386(self):
        from ml.recommendation.bigquery_client import fetch_all_embeddings
        client = make_mock_client(query_rows=[make_bq_row("vid_0")])
        result = fetch_all_embeddings(client)
        assert result["embedding"].iloc[0].shape == (386,)

    def test_empty_result_returns_empty_df(self):
        from ml.recommendation.bigquery_client import fetch_all_embeddings
        client = make_mock_client(query_rows=[])
        result = fetch_all_embeddings(client)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_video_ids_correct(self):
        from ml.recommendation.bigquery_client import fetch_all_embeddings
        rows   = [make_bq_row(f"vid_{i}") for i in range(3)]
        client = make_mock_client(query_rows=rows)
        result = fetch_all_embeddings(client)
        assert list(result["video_id"]) == ["vid_0", "vid_1", "vid_2"]

    def test_query_called_with_table_ref(self):
        from ml.recommendation.bigquery_client import fetch_all_embeddings, TABLE_REF
        client = make_mock_client()
        fetch_all_embeddings(client)
        query_str = client.query.call_args[0][0]
        assert TABLE_REF in query_str

    def test_no_nan_in_embeddings(self):
        from ml.recommendation.bigquery_client import fetch_all_embeddings
        rows   = [make_bq_row(f"vid_{i}") for i in range(3)]
        client = make_mock_client(query_rows=rows)
        result = fetch_all_embeddings(client)
        for emb in result["embedding"]:
            assert not np.isnan(emb).any()


# ── get_existing_video_ids tests ──────────────────────────────────────────────

class TestGetExistingVideoIds:

    def test_returns_set(self):
        from ml.recommendation.bigquery_client import get_existing_video_ids
        rows   = [MagicMock(video_id=f"vid_{i}") for i in range(3)]
        client = make_mock_client(query_rows=rows)
        result = get_existing_video_ids(client)
        assert isinstance(result, set)

    def test_returns_correct_ids(self):
        from ml.recommendation.bigquery_client import get_existing_video_ids
        rows   = [MagicMock(video_id="vid_0"), MagicMock(video_id="vid_1")]
        client = make_mock_client(query_rows=rows)
        result = get_existing_video_ids(client)
        assert result == {"vid_0", "vid_1"}

    def test_empty_table_returns_empty_set(self):
        from ml.recommendation.bigquery_client import get_existing_video_ids
        client = make_mock_client(query_rows=[])
        result = get_existing_video_ids(client)
        assert result == set()

    def test_exception_returns_empty_set(self):
        from ml.recommendation.bigquery_client import get_existing_video_ids
        client = MagicMock()
        client.query.side_effect = Exception("error")
        result = get_existing_video_ids(client)
        assert result == set()


# ── load_embeddings tests ─────────────────────────────────────────────────────

class TestLoadEmbeddings:

    def test_inserts_new_songs(self):
        from ml.recommendation.bigquery_client import load_embeddings
        df     = make_embedding_df(5)
        client = make_mock_client(query_rows=[])
        load_embeddings(client, df)
        client.insert_rows_json.assert_called_once()

    def test_skips_existing_songs(self):
        from ml.recommendation.bigquery_client import load_embeddings
        df       = make_embedding_df(3)
        existing = [MagicMock(video_id=f"vid_{i}") for i in range(3)]
        client   = make_mock_client(query_rows=existing)
        load_embeddings(client, df)
        client.insert_rows_json.assert_not_called()

    def test_empty_df_no_insert(self):
        from ml.recommendation.bigquery_client import load_embeddings
        df     = make_embedding_df(0)
        client = make_mock_client(query_rows=[])
        load_embeddings(client, df)
        client.insert_rows_json.assert_not_called()

    def test_inserts_only_new_songs(self):
        from ml.recommendation.bigquery_client import load_embeddings
        df       = make_embedding_df(5)
        existing = [MagicMock(video_id="vid_0"), MagicMock(video_id="vid_1")]
        client   = make_mock_client(query_rows=existing)
        load_embeddings(client, df)
        inserted     = client.insert_rows_json.call_args[0][1]
        inserted_ids = {r["video_id"] for r in inserted}
        assert "vid_0" not in inserted_ids
        assert "vid_1" not in inserted_ids

    def test_correct_count_inserted(self):
        from ml.recommendation.bigquery_client import load_embeddings
        df       = make_embedding_df(5)
        existing = [MagicMock(video_id="vid_0")]
        client   = make_mock_client(query_rows=existing)
        load_embeddings(client, df)
        inserted = client.insert_rows_json.call_args[0][1]
        assert len(inserted) == 4


# ── get_user_liked_songs tests ────────────────────────────────────────────────

class TestGetUserLikedSongs:

    def test_returns_dict(self):
        from ml.recommendation.bigquery_client import get_user_liked_songs
        rows   = [make_user_row("u1", liked=["vid_0", "vid_1"])]
        client = make_mock_client(query_rows=rows)
        result = get_user_liked_songs(client, ["u1"])
        assert isinstance(result, dict)

    def test_correct_liked_songs(self):
        from ml.recommendation.bigquery_client import get_user_liked_songs
        rows   = [make_user_row("u1", liked=["vid_0", "vid_1"])]
        client = make_mock_client(query_rows=rows)
        result = get_user_liked_songs(client, ["u1"])
        assert result["u1"] == ["vid_0", "vid_1"]

    def test_empty_user_ids_returns_empty(self):
        from ml.recommendation.bigquery_client import get_user_liked_songs
        client = make_mock_client()
        result = get_user_liked_songs(client, [])
        assert result == {}

    def test_user_with_no_liked_songs_excluded(self):
        from ml.recommendation.bigquery_client import get_user_liked_songs
        rows   = [make_user_row("u1", liked=[])]
        client = make_mock_client(query_rows=rows)
        result = get_user_liked_songs(client, ["u1"])
        assert "u1" not in result

    def test_multiple_users(self):
        from ml.recommendation.bigquery_client import get_user_liked_songs
        rows = [
            make_user_row("u1", liked=["vid_0"]),
            make_user_row("u2", liked=["vid_1", "vid_2"]),
        ]
        client = make_mock_client(query_rows=rows)
        result = get_user_liked_songs(client, ["u1", "u2"])
        assert "u1" in result
        assert "u2" in result

    def test_exception_returns_empty(self):
        from ml.recommendation.bigquery_client import get_user_liked_songs
        client = MagicMock()
        client.query.side_effect = Exception("error")
        result = get_user_liked_songs(client, ["u1"])
        assert result == {}

    def test_query_includes_user_ids(self):
        from ml.recommendation.bigquery_client import get_user_liked_songs
        client = make_mock_client(query_rows=[])
        get_user_liked_songs(client, ["u1", "u2"])
        query_str = client.query.call_args[0][0]
        assert "u1" in query_str
        assert "u2" in query_str

    def test_query_includes_users_table_ref(self):
        from ml.recommendation.bigquery_client import get_user_liked_songs, USERS_TABLE_REF
        client = make_mock_client(query_rows=[])
        get_user_liked_songs(client, ["u1"])
        query_str = client.query.call_args[0][0]
        assert USERS_TABLE_REF in query_str

    def test_liked_songs_is_list(self):
        from ml.recommendation.bigquery_client import get_user_liked_songs
        rows   = [make_user_row("u1", liked=["vid_0", "vid_1"])]
        client = make_mock_client(query_rows=rows)
        result = get_user_liked_songs(client, ["u1"])
        assert isinstance(result["u1"], list)


# ── get_user_disliked_songs tests ─────────────────────────────────────────────

class TestGetUserDislikedSongs:

    def test_returns_dict(self):
        from ml.recommendation.bigquery_client import get_user_disliked_songs
        rows   = [make_user_row("u1", disliked=["vid_x"])]
        client = make_mock_client(query_rows=rows)
        result = get_user_disliked_songs(client, ["u1"])
        assert isinstance(result, dict)

    def test_correct_disliked_songs(self):
        from ml.recommendation.bigquery_client import get_user_disliked_songs
        rows   = [make_user_row("u1", disliked=["vid_x", "vid_y"])]
        client = make_mock_client(query_rows=rows)
        result = get_user_disliked_songs(client, ["u1"])
        assert result["u1"] == ["vid_x", "vid_y"]

    def test_empty_user_ids_returns_empty(self):
        from ml.recommendation.bigquery_client import get_user_disliked_songs
        client = make_mock_client()
        result = get_user_disliked_songs(client, [])
        assert result == {}

    def test_user_with_no_disliked_excluded(self):
        from ml.recommendation.bigquery_client import get_user_disliked_songs
        rows   = [make_user_row("u1", disliked=[])]
        client = make_mock_client(query_rows=rows)
        result = get_user_disliked_songs(client, ["u1"])
        assert "u1" not in result

    def test_multiple_users(self):
        from ml.recommendation.bigquery_client import get_user_disliked_songs
        rows = [
            make_user_row("u1", disliked=["vid_a"]),
            make_user_row("u2", disliked=["vid_b"]),
        ]
        client = make_mock_client(query_rows=rows)
        result = get_user_disliked_songs(client, ["u1", "u2"])
        assert "u1" in result and "u2" in result

    def test_exception_returns_empty(self):
        from ml.recommendation.bigquery_client import get_user_disliked_songs
        client = MagicMock()
        client.query.side_effect = Exception("error")
        result = get_user_disliked_songs(client, ["u1"])
        assert result == {}

    def test_disliked_songs_is_list(self):
        from ml.recommendation.bigquery_client import get_user_disliked_songs
        rows   = [make_user_row("u1", disliked=["vid_x"])]
        client = make_mock_client(query_rows=rows)
        result = get_user_disliked_songs(client, ["u1"])
        assert isinstance(result["u1"], list)

    def test_query_includes_users_table_ref(self):
        from ml.recommendation.bigquery_client import (
            get_user_disliked_songs, USERS_TABLE_REF
        )
        client = make_mock_client(query_rows=[])
        get_user_disliked_songs(client, ["u1"])
        query_str = client.query.call_args[0][0]
        assert USERS_TABLE_REF in query_str


# ── Live GCP tests (skipped in CI) ────────────────────────────────────────────

@pytest.mark.skipif(os.getenv("CI") == "true", reason="Skipped in CI — requires GCP")
def test_fetch_all_embeddings_live():
    from ml.recommendation.bigquery_client import get_client, fetch_all_embeddings
    client = get_client()
    df     = fetch_all_embeddings(client)
    assert not df.empty
    assert "embedding" in df.columns


@pytest.mark.skipif(os.getenv("CI") == "true", reason="Skipped in CI — requires GCP")
def test_get_user_liked_songs_live():
    from ml.recommendation.bigquery_client import get_client, get_user_liked_songs
    client = get_client()
    result = get_user_liked_songs(client, ["test_user_001"])
    assert isinstance(result, dict)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
