"""
Tests for feature_engineering/bigquery_loader.py

Covers:
  - get_client()
  - ensure_table_exists()
  - get_existing_video_ids()
  - load_embeddings()
  - edge cases: empty df, all duplicates, partial duplicates, insert errors
"""

import os
import pytest
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock, call


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_embedding_df(n=5, seed=0):
    np.random.seed(seed)
    return pd.DataFrame({
        "video_id":        [f"vid_{i}" for i in range(n)],
        "track_title":     [f"Song {i}" for i in range(n)],
        "artist_name":     [f"Artist {i}" for i in range(n)],
        "genre":           ["pop"] * n,
        "duration_sec":    [200.0 + i for i in range(n)],
        "popularity_score": [0.5] * n,
        "embedding":       [np.random.rand(386).tolist() for _ in range(n)],
    })


def make_mock_client(existing_ids=None, insert_errors=None):
    client = MagicMock()
    client.get_table.return_value = MagicMock()
    rows = [MagicMock(video_id=vid) for vid in (existing_ids or [])]
    client.query.return_value.result.return_value = rows
    client.insert_rows_json.return_value = insert_errors or []
    return client


# ── get_client tests ──────────────────────────────────────────────────────────

class TestGetClient:

    @patch("ml.feature_engineering.bigquery_loader.bigquery.Client")
    def test_returns_client(self, mock_bq):
        from ml.feature_engineering.bigquery_loader import get_client
        mock_bq.return_value = MagicMock()
        client = get_client()
        assert client is not None

    @patch("ml.feature_engineering.bigquery_loader.bigquery.Client")
    def test_uses_project_id(self, mock_bq):
        from ml.feature_engineering.bigquery_loader import get_client
        from ml.recommendation.config import PROJECT_ID   # ← fixed import
        get_client()
        mock_bq.assert_called_once_with(project=PROJECT_ID)

    @patch("ml.feature_engineering.bigquery_loader.bigquery.Client")
    def test_called_once(self, mock_bq):
        from ml.feature_engineering.bigquery_loader import get_client
        get_client()
        assert mock_bq.call_count == 1


# ── ensure_table_exists tests ─────────────────────────────────────────────────

class TestEnsureTableExists:

    def test_does_not_create_if_table_exists(self):
        from ml.feature_engineering.bigquery_loader import ensure_table_exists
        client = make_mock_client()
        df     = make_embedding_df()
        ensure_table_exists(client, df)
        client.create_table.assert_not_called()

    def test_creates_table_if_not_exists(self):
        from ml.feature_engineering.bigquery_loader import ensure_table_exists
        client = make_mock_client()
        client.get_table.side_effect = Exception("Table not found")
        df = make_embedding_df()
        ensure_table_exists(client, df)
        client.create_table.assert_called_once()

    def test_table_schema_has_video_id(self):
        from ml.feature_engineering.bigquery_loader import ensure_table_exists
        client = make_mock_client()
        client.get_table.side_effect = Exception("not found")
        df = make_embedding_df()
        ensure_table_exists(client, df)
        schema = client.create_table.call_args[0][0].schema
        field_names = [f.name for f in schema]
        assert "video_id" in field_names

    def test_table_schema_has_embedding(self):
        from ml.feature_engineering.bigquery_loader import ensure_table_exists
        client = make_mock_client()
        client.get_table.side_effect = Exception("not found")
        df = make_embedding_df()
        ensure_table_exists(client, df)
        schema = client.create_table.call_args[0][0].schema
        field_names = [f.name for f in schema]
        assert "embedding" in field_names

    def test_table_schema_has_track_title(self):
        from ml.feature_engineering.bigquery_loader import ensure_table_exists
        client = make_mock_client()
        client.get_table.side_effect = Exception("not found")
        df = make_embedding_df()
        ensure_table_exists(client, df)
        schema = client.create_table.call_args[0][0].schema
        field_names = [f.name for f in schema]
        assert "track_title" in field_names

    def test_table_schema_has_duration_sec(self):
        from ml.feature_engineering.bigquery_loader import ensure_table_exists
        client = make_mock_client()
        client.get_table.side_effect = Exception("not found")
        df = make_embedding_df()
        ensure_table_exists(client, df)
        schema = client.create_table.call_args[0][0].schema
        field_names = [f.name for f in schema]
        assert "duration_sec" in field_names

    def test_table_schema_has_popularity_score(self):
        from ml.feature_engineering.bigquery_loader import ensure_table_exists
        client = make_mock_client()
        client.get_table.side_effect = Exception("not found")
        df = make_embedding_df()
        ensure_table_exists(client, df)
        schema = client.create_table.call_args[0][0].schema
        field_names = [f.name for f in schema]
        assert "popularity_score" in field_names

    def test_video_id_is_required(self):
        from ml.feature_engineering.bigquery_loader import ensure_table_exists
        client = make_mock_client()
        client.get_table.side_effect = Exception("not found")
        df = make_embedding_df()
        ensure_table_exists(client, df)
        schema      = client.create_table.call_args[0][0].schema
        video_field = next(f for f in schema if f.name == "video_id")
        assert video_field.mode == "REQUIRED"

    def test_embedding_is_repeated(self):
        from ml.feature_engineering.bigquery_loader import ensure_table_exists
        client = make_mock_client()
        client.get_table.side_effect = Exception("not found")
        df = make_embedding_df()
        ensure_table_exists(client, df)
        schema    = client.create_table.call_args[0][0].schema
        emb_field = next(f for f in schema if f.name == "embedding")
        assert emb_field.mode == "REPEATED"

    def test_get_table_called_with_table_ref(self):
        from ml.feature_engineering.bigquery_loader import ensure_table_exists, TABLE_REF
        client = make_mock_client()
        df     = make_embedding_df()
        ensure_table_exists(client, df)
        client.get_table.assert_called_once_with(TABLE_REF)


# ── get_existing_video_ids tests ──────────────────────────────────────────────

class TestGetExistingVideoIds:

    def test_returns_set(self):
        from ml.feature_engineering.bigquery_loader import get_existing_video_ids
        client = make_mock_client(existing_ids=["vid_0", "vid_1"])
        result = get_existing_video_ids(client)
        assert isinstance(result, set)

    def test_returns_correct_ids(self):
        from ml.feature_engineering.bigquery_loader import get_existing_video_ids
        client = make_mock_client(existing_ids=["vid_0", "vid_1"])
        result = get_existing_video_ids(client)
        assert result == {"vid_0", "vid_1"}

    def test_empty_table_returns_empty_set(self):
        from ml.feature_engineering.bigquery_loader import get_existing_video_ids
        client = make_mock_client(existing_ids=[])
        result = get_existing_video_ids(client)
        assert result == set()

    def test_query_exception_returns_empty_set(self):
        from ml.feature_engineering.bigquery_loader import get_existing_video_ids
        client = MagicMock()
        client.query.side_effect = Exception("Query failed")
        result = get_existing_video_ids(client)
        assert result == set()

    def test_returns_all_ids(self):
        from ml.feature_engineering.bigquery_loader import get_existing_video_ids
        ids    = [f"vid_{i}" for i in range(20)]
        client = make_mock_client(existing_ids=ids)
        result = get_existing_video_ids(client)
        assert result == set(ids)

    def test_no_duplicates_in_result(self):
        from ml.feature_engineering.bigquery_loader import get_existing_video_ids
        client = make_mock_client(existing_ids=["vid_0", "vid_0", "vid_1"])
        result = get_existing_video_ids(client)
        assert len(result) == len(set(result))

    def test_query_called_with_table_ref(self):
        from ml.feature_engineering.bigquery_loader import get_existing_video_ids, TABLE_REF
        client = make_mock_client()
        get_existing_video_ids(client)
        query_str = client.query.call_args[0][0]
        assert TABLE_REF in query_str


# ── load_embeddings tests ─────────────────────────────────────────────────────

class TestLoadEmbeddings:

    def test_inserts_new_songs(self):
        from ml.feature_engineering.bigquery_loader import load_embeddings
        df     = make_embedding_df(5)
        client = make_mock_client(existing_ids=[])
        load_embeddings(client, df)
        client.insert_rows_json.assert_called_once()

    def test_skips_existing_songs(self):
        from ml.feature_engineering.bigquery_loader import load_embeddings
        df       = make_embedding_df(5)
        existing = [f"vid_{i}" for i in range(5)]
        client   = make_mock_client(existing_ids=existing)
        load_embeddings(client, df)
        client.insert_rows_json.assert_not_called()

    def test_inserts_only_new_songs(self):
        from ml.feature_engineering.bigquery_loader import load_embeddings
        df     = make_embedding_df(5)
        client = make_mock_client(existing_ids=["vid_0", "vid_1"])
        load_embeddings(client, df)
        inserted     = client.insert_rows_json.call_args[0][1]
        inserted_ids = [r["video_id"] for r in inserted]
        assert "vid_0" not in inserted_ids
        assert "vid_1" not in inserted_ids

    def test_empty_df_no_insert(self):
        from ml.feature_engineering.bigquery_loader import load_embeddings
        df     = make_embedding_df(0)
        client = make_mock_client(existing_ids=[])
        load_embeddings(client, df)
        client.insert_rows_json.assert_not_called()

    def test_insert_errors_logged(self, capsys):
        from ml.feature_engineering.bigquery_loader import load_embeddings
        df     = make_embedding_df(3)
        client = make_mock_client(existing_ids=[], insert_errors=[{"error": "bad"}])
        load_embeddings(client, df)
        captured = capsys.readouterr()
        assert "Errors" in captured.out or "error" in captured.out.lower()

    def test_no_errors_on_success(self, capsys):
        from ml.feature_engineering.bigquery_loader import load_embeddings
        df     = make_embedding_df(3)
        client = make_mock_client(existing_ids=[], insert_errors=[])
        load_embeddings(client, df)
        captured = capsys.readouterr()
        assert "Errors" not in captured.out

    def test_correct_number_of_rows_inserted(self):
        from ml.feature_engineering.bigquery_loader import load_embeddings
        df     = make_embedding_df(5)
        client = make_mock_client(existing_ids=["vid_0"])
        load_embeddings(client, df)
        inserted = client.insert_rows_json.call_args[0][1]
        assert len(inserted) == 4

    def test_all_songs_new_inserts_all(self):
        from ml.feature_engineering.bigquery_loader import load_embeddings
        df     = make_embedding_df(5)
        client = make_mock_client(existing_ids=[])
        load_embeddings(client, df)
        inserted = client.insert_rows_json.call_args[0][1]
        assert len(inserted) == 5

    def test_ensure_table_called(self):
        from ml.feature_engineering.bigquery_loader import load_embeddings
        df     = make_embedding_df(3)
        client = make_mock_client(existing_ids=[])
        load_embeddings(client, df)
        client.get_table.assert_called()

    def test_partial_overlap_inserts_correct_subset(self):
        from ml.feature_engineering.bigquery_loader import load_embeddings
        df     = make_embedding_df(6)
        client = make_mock_client(existing_ids=["vid_0", "vid_2", "vid_4"])
        load_embeddings(client, df)
        inserted     = client.insert_rows_json.call_args[0][1]
        inserted_ids = {r["video_id"] for r in inserted}
        assert inserted_ids == {"vid_1", "vid_3", "vid_5"}

    def test_insert_rows_json_called_with_table_ref(self):
        from ml.feature_engineering.bigquery_loader import load_embeddings, TABLE_REF
        df     = make_embedding_df(2)
        client = make_mock_client(existing_ids=[])
        load_embeddings(client, df)
        called_ref = client.insert_rows_json.call_args[0][0]
        assert called_ref == TABLE_REF

    def test_single_new_song_inserted(self):
        from ml.feature_engineering.bigquery_loader import load_embeddings
        df     = make_embedding_df(1)
        client = make_mock_client(existing_ids=[])
        load_embeddings(client, df)
        inserted = client.insert_rows_json.call_args[0][1]
        assert len(inserted) == 1
        assert inserted[0]["video_id"] == "vid_0"

    def test_large_batch_all_new(self):
        from ml.feature_engineering.bigquery_loader import load_embeddings
        df     = make_embedding_df(100)
        client = make_mock_client(existing_ids=[])
        load_embeddings(client, df)
        inserted = client.insert_rows_json.call_args[0][1]
        assert len(inserted) == 100

    def test_large_batch_all_existing(self):
        from ml.feature_engineering.bigquery_loader import load_embeddings
        df       = make_embedding_df(100)
        existing = [f"vid_{i}" for i in range(100)]
        client   = make_mock_client(existing_ids=existing)
        load_embeddings(client, df)
        client.insert_rows_json.assert_not_called()


# ── Config / TABLE_REF tests ──────────────────────────────────────────────────

class TestBigqueryLoaderConfig:

    def test_table_ref_contains_project_id(self):
        from ml.feature_engineering.bigquery_loader import TABLE_REF
        from ml.recommendation.config import PROJECT_ID   # ← fixed import
        assert PROJECT_ID in TABLE_REF

    def test_table_ref_is_string(self):
        from ml.feature_engineering.bigquery_loader import TABLE_REF
        assert isinstance(TABLE_REF, str)

    def test_project_id_is_string(self):
        from ml.recommendation.config import PROJECT_ID   # ← fixed import
        assert isinstance(PROJECT_ID, str)

    def test_project_id_not_empty(self):
        from ml.recommendation.config import PROJECT_ID   # ← fixed import
        assert len(PROJECT_ID) > 0

    def test_table_ref_has_three_parts(self):
        from ml.feature_engineering.bigquery_loader import TABLE_REF
        parts = TABLE_REF.split(".")
        assert len(parts) == 3

    def test_table_ref_matches_config(self):
        from ml.feature_engineering.bigquery_loader import TABLE_REF
        from ml.recommendation.config import TABLE_REF as config_table_ref
        assert TABLE_REF == config_table_ref


# ── Live GCP tests (skipped in CI) ────────────────────────────────────────────

@pytest.mark.skipif(os.getenv("CI") == "true", reason="Skipped in CI — requires GCP")
def test_get_existing_video_ids_live():
    from ml.feature_engineering.bigquery_loader import get_client, get_existing_video_ids
    client = get_client()
    result = get_existing_video_ids(client)
    assert isinstance(result, set)


@pytest.mark.skipif(os.getenv("CI") == "true", reason="Skipped in CI — requires GCP")
def test_load_embeddings_live():
    from ml.feature_engineering.bigquery_loader import get_client, load_embeddings
    df = make_embedding_df(1)
    df["video_id"] = ["test_video_pytest_do_not_keep"]
    client = get_client()
    load_embeddings(client, df)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])