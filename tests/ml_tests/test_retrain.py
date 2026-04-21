"""
Unit tests for ml.ct_cm.retrain
All tests run without any GCP dependencies.
Firestore, GCS, BigQuery and Cloud Monitoring clients are mocked.
"""
import io
import json
import numpy as np
import pandas as pd
import pytest
import torch
from unittest.mock import MagicMock, patch
from ml.ct_cm.retrain import (
    fetch_real_sessions,
    build_training_samples,
    get_production_val_loss,
    run_retraining,
    MIN_SESSIONS_REQUIRED,
    MIN_SONGS_PER_SESSION,
    IMPROVEMENT_THRESHOLD,
    GCS_BUCKET,
    MODEL_BLOB,
)

DIM = 386


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_embedding_lookup(video_ids: list) -> dict:
    rng = np.random.default_rng(42)
    return {
        vid: rng.random(DIM).astype(np.float32)
        for vid in video_ids
    }


def _make_mock_db(sessions: list) -> MagicMock:
    db = MagicMock()
    session_docs = []

    for session_data, song_events in sessions:
        session_doc = MagicMock()
        session_doc.id = session_data.get("session_id", "test_session")
        session_doc.to_dict.return_value = session_data

        evt_docs = []
        for evt in song_events:
            evt_doc = MagicMock()
            evt_doc.to_dict.return_value = evt
            evt_docs.append(evt_doc)

        db.collection.return_value.document.return_value \
          .collection.return_value.order_by.return_value \
          .stream.return_value = evt_docs

        session_docs.append(session_doc)

    db.collection.return_value.stream.return_value = session_docs
    return db


def _make_songs_df(n: int = 10) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "video_id":    [f"vid_{i}" for i in range(n)],
        "track_title": [f"Track {i}" for i in range(n)],
        "artist_name": [f"Artist {i}" for i in range(n)],
        "genre":       ["chill"] * n,
        "embedding":   [rng.random(DIM).astype(np.float32).tolist() for _ in range(n)],
    })


def _make_enough_sessions(n: int = MIN_SESSIONS_REQUIRED) -> list:
    return [
        ({"status": "ended", "session_id": f"s{i}"}, [
            {"video_id": f"v{j}", "liked_flag": 0}
            for j in range(MIN_SONGS_PER_SESSION + 1)
        ])
        for i in range(n)
    ]


# ── fetch_real_sessions Tests ─────────────────────────────────────────────────

def test_fetch_real_sessions_empty_db():
    db = MagicMock()
    db.collection.return_value.stream.return_value = []
    result = fetch_real_sessions(db)
    assert result == []


def test_fetch_real_sessions_filters_active_sessions():
    db = _make_mock_db([
        ({"status": "active", "session_id": "s1"}, [
            {"video_id": f"v{i}", "liked_flag": 0} for i in range(10)
        ]),
    ])
    result = fetch_real_sessions(db)
    assert result == []


def test_fetch_real_sessions_filters_short_sessions():
    short_events = [
        {"video_id": f"v{i}", "liked_flag": 0}
        for i in range(MIN_SONGS_PER_SESSION - 1)
    ]
    db = _make_mock_db([
        ({"status": "ended", "session_id": "s1"}, short_events),
    ])
    result = fetch_real_sessions(db)
    assert result == []


def test_fetch_real_sessions_includes_valid_sessions():
    events = [
        {"video_id": f"v{i}", "liked_flag": 0}
        for i in range(MIN_SONGS_PER_SESSION + 2)
    ]
    db = _make_mock_db([
        ({"status": "ended", "session_id": "s1"}, events),
    ])
    result = fetch_real_sessions(db)
    assert len(result) == 1
    assert len(result[0]) == MIN_SONGS_PER_SESSION + 2


def test_fetch_real_sessions_includes_done_status():
    events = [
        {"video_id": f"v{i}", "liked_flag": 0}
        for i in range(MIN_SONGS_PER_SESSION + 1)
    ]
    db = _make_mock_db([
        ({"status": "done", "session_id": "s1"}, events),
    ])
    result = fetch_real_sessions(db)
    assert len(result) == 1


def test_fetch_real_sessions_filters_empty_video_ids():
    events = [
        {"video_id": "", "liked_flag": 0},
        {"video_id": "v1", "liked_flag": 0},
        {"video_id": "v2", "liked_flag": 1},
        {"video_id": "v3", "liked_flag": 0},
        {"video_id": "v4", "liked_flag": 0},
        {"video_id": "v5", "liked_flag": 0},
    ]
    db = _make_mock_db([
        ({"status": "ended", "session_id": "s1"}, events),
    ])
    result = fetch_real_sessions(db)
    assert len(result) == 1
    assert all(e["video_id"] != "" for e in result[0])


def test_fetch_real_sessions_returns_correct_structure():
    events = [
        {"video_id": f"v{i}", "liked_flag": i % 2}
        for i in range(MIN_SONGS_PER_SESSION + 1)
    ]
    db = _make_mock_db([
        ({"status": "ended", "session_id": "s1"}, events),
    ])
    result = fetch_real_sessions(db)
    assert len(result) == 1
    for evt in result[0]:
        assert "video_id" in evt
        assert "liked_flag" in evt


def test_fetch_real_sessions_multiple_sessions():
    events = [
        {"video_id": f"v{i}", "liked_flag": 0}
        for i in range(MIN_SONGS_PER_SESSION + 1)
    ]
    db = _make_mock_db([
        ({"status": "ended", "session_id": "s1"}, events),
        ({"status": "ended", "session_id": "s2"}, events),
        ({"status": "active", "session_id": "s3"}, events),
    ])
    result = fetch_real_sessions(db)
    assert len(result) == 2


# ── build_training_samples Tests ──────────────────────────────────────────────

def test_build_training_samples_empty_sessions():
    result = build_training_samples([], {})
    assert result == []


def test_build_training_samples_filters_unknown_ids():
    sessions = [
        [
            {"video_id": "unknown_1", "liked_flag": 0},
            {"video_id": "unknown_2", "liked_flag": 0},
        ]
    ]
    result = build_training_samples(sessions, {})
    assert result == []


def test_build_training_samples_generates_subsequences():
    video_ids = [f"v{i}" for i in range(5)]
    lookup    = _make_embedding_lookup(video_ids)
    sessions  = [[{"video_id": vid, "liked_flag": 0} for vid in video_ids]]
    result    = build_training_samples(sessions, lookup)
    assert len(result) == 4


def test_build_training_samples_returns_tensors():
    video_ids = [f"v{i}" for i in range(3)]
    lookup    = _make_embedding_lookup(video_ids)
    sessions  = [[{"video_id": vid, "liked_flag": 0} for vid in video_ids]]
    result    = build_training_samples(sessions, lookup)
    assert len(result) > 0
    input_tensor, target_tensor = result[0]
    assert isinstance(input_tensor, torch.Tensor)
    assert isinstance(target_tensor, torch.Tensor)


def test_build_training_samples_input_shape():
    video_ids = [f"v{i}" for i in range(5)]
    lookup    = _make_embedding_lookup(video_ids)
    sessions  = [[{"video_id": vid, "liked_flag": 0} for vid in video_ids]]
    result    = build_training_samples(sessions, lookup)
    input_tensor, _ = result[0]
    assert input_tensor.shape == (15, DIM + 1)


def test_build_training_samples_target_shape():
    video_ids = [f"v{i}" for i in range(3)]
    lookup    = _make_embedding_lookup(video_ids)
    sessions  = [[{"video_id": vid, "liked_flag": 0} for vid in video_ids]]
    result    = build_training_samples(sessions, lookup)
    _, target_tensor = result[0]
    assert target_tensor.shape == (DIM,)


def test_build_training_samples_liked_flag_in_input():
    video_ids = ["v0", "v1", "v2"]
    lookup    = _make_embedding_lookup(video_ids)
    sessions  = [
        [
            {"video_id": "v0", "liked_flag": 1},
            {"video_id": "v1", "liked_flag": 0},
            {"video_id": "v2", "liked_flag": 1},
        ]
    ]
    result = build_training_samples(sessions, lookup)
    input_tensor, _ = result[0]
    assert input_tensor[-1, -1].item() == pytest.approx(1.0, abs=1e-4)


def test_build_training_samples_multiple_sessions():
    video_ids = [f"v{i}" for i in range(4)]
    lookup    = _make_embedding_lookup(video_ids)
    sessions  = [
        [{"video_id": vid, "liked_flag": 0} for vid in video_ids],
        [{"video_id": vid, "liked_flag": 1} for vid in video_ids],
    ]
    result = build_training_samples(sessions, lookup)
    assert len(result) == 6


# ── get_production_val_loss Tests ─────────────────────────────────────────────

@patch("ml.ct_cm.retrain.storage.Client")
def test_get_production_val_loss_no_history(mock_storage):
    mock_blob = MagicMock()
    mock_blob.exists.return_value = False
    mock_storage.return_value.bucket.return_value.blob.return_value = mock_blob
    result = get_production_val_loss()
    assert result == float("inf")


@patch("ml.ct_cm.retrain.storage.Client")
def test_get_production_val_loss_with_history(mock_storage):
    history = {"production_val_loss": 0.3226, "timestamp": "2026-04-15"}
    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_blob.download_as_text.return_value = json.dumps(history)
    mock_storage.return_value.bucket.return_value.blob.return_value = mock_blob
    result = get_production_val_loss()
    assert result == pytest.approx(0.3226, abs=1e-4)


@patch("ml.ct_cm.retrain.storage.Client")
def test_get_production_val_loss_gcs_error(mock_storage):
    mock_storage.side_effect = Exception("GCS connection failed")
    result = get_production_val_loss()
    assert result == float("inf")


# ── run_retraining Tests ──────────────────────────────────────────────────────

@patch("ml.ct_cm.retrain.write_retrain_event")
@patch("ml.ct_cm.retrain.firestore.Client")
def test_run_retraining_skips_insufficient_sessions(mock_fs, mock_write):
    """Skips retraining when fewer than MIN_SESSIONS_REQUIRED sessions."""
    mock_db = _make_mock_db([
        ({"status": "ended", "session_id": f"s{i}"}, [
            {"video_id": f"v{j}", "liked_flag": 0}
            for j in range(MIN_SONGS_PER_SESSION + 1)
        ])
        for i in range(MIN_SESSIONS_REQUIRED - 1)
    ])
    mock_fs.return_value = mock_db

    result = run_retraining()

    assert result["status"] == "skipped"
    assert result["deployed"] is False
    assert "sessions available" in result["reason"]
    mock_write.assert_called_once()


@patch("ml.ct_cm.retrain.write_retrain_event")
@patch("ml.ct_cm.retrain.build_embedding_lookup")
@patch("ml.ct_cm.retrain.fetch_all_embeddings")
@patch("ml.ct_cm.retrain.get_client")
@patch("ml.ct_cm.retrain.firestore.Client")
def test_run_retraining_skips_no_valid_samples(
    mock_fs, mock_bq, mock_fetch_embeddings,
    mock_build_lookup, mock_write
):
    """Skips retraining when no valid training samples after embedding lookup."""
    mock_fs.return_value = MagicMock()
    mock_bq.return_value = MagicMock()
    mock_fetch_embeddings.return_value = _make_songs_df()
    mock_build_lookup.return_value = {}  # empty lookup

    with patch("ml.ct_cm.retrain.fetch_real_sessions",
               return_value=[[{"video_id": f"unknown_{j}", "liked_flag": 0}
                               for j in range(MIN_SONGS_PER_SESSION + 1)]]
                              * MIN_SESSIONS_REQUIRED):
        result = run_retraining()

    assert result["status"] == "skipped"
    assert result["deployed"] is False


@patch("ml.ct_cm.retrain.publish_retrain_metrics")
@patch("ml.ct_cm.retrain.save_retrain_history")
@patch("ml.ct_cm.retrain.write_retrain_event")
@patch("ml.ct_cm.retrain.deploy_model")
@patch("ml.ct_cm.retrain.get_production_val_loss")
@patch("ml.ct_cm.retrain.build_embedding_lookup")
@patch("ml.ct_cm.retrain.fetch_all_embeddings")
@patch("ml.ct_cm.retrain.get_client")
@patch("ml.ct_cm.retrain.firestore.Client")
def test_run_retraining_deploys_when_improved(
    mock_fs, mock_bq, mock_fetch_embeddings, mock_build_lookup,
    mock_prod_loss, mock_deploy, mock_write, mock_save, mock_publish
):
    """Deploys new model when val_loss improves beyond threshold."""
    mock_fs.return_value = MagicMock()
    mock_bq.return_value = MagicMock()
    mock_prod_loss.return_value = 0.5

    video_ids = [f"v{i}" for i in range(10)]
    lookup    = _make_embedding_lookup(video_ids)
    mock_fetch_embeddings.return_value = _make_songs_df(10)
    mock_build_lookup.return_value = lookup

    with patch("ml.ct_cm.retrain.fetch_real_sessions",
               return_value=[[{"video_id": vid, "liked_flag": 0}
                               for vid in video_ids]] * MIN_SESSIONS_REQUIRED):
        with patch("ml.ct_cm.retrain.train",
                   return_value=(MagicMock(), 0.3)):
            result = run_retraining()

    assert result["deployed"] is True
    assert result["status"] == "completed"
    assert result["new_val_loss"] == pytest.approx(0.3, abs=1e-4)
    mock_deploy.assert_called_once()


@patch("ml.ct_cm.retrain.publish_retrain_metrics")
@patch("ml.ct_cm.retrain.save_retrain_history")
@patch("ml.ct_cm.retrain.write_retrain_event")
@patch("ml.ct_cm.retrain.deploy_model")
@patch("ml.ct_cm.retrain.get_production_val_loss")
@patch("ml.ct_cm.retrain.build_embedding_lookup")
@patch("ml.ct_cm.retrain.fetch_all_embeddings")
@patch("ml.ct_cm.retrain.get_client")
@patch("ml.ct_cm.retrain.firestore.Client")
def test_run_retraining_does_not_deploy_when_not_improved(
    mock_fs, mock_bq, mock_fetch_embeddings, mock_build_lookup,
    mock_prod_loss, mock_deploy, mock_write, mock_save, mock_publish
):
    """Does not deploy when improvement is below threshold."""
    mock_fs.return_value = MagicMock()
    mock_bq.return_value = MagicMock()
    mock_prod_loss.return_value = 0.32

    video_ids = [f"v{i}" for i in range(10)]
    lookup    = _make_embedding_lookup(video_ids)
    mock_fetch_embeddings.return_value = _make_songs_df(10)
    mock_build_lookup.return_value = lookup

    with patch("ml.ct_cm.retrain.fetch_real_sessions",
               return_value=[[{"video_id": vid, "liked_flag": 0}
                               for vid in video_ids]] * MIN_SESSIONS_REQUIRED):
        with patch("ml.ct_cm.retrain.train",
                   return_value=(MagicMock(), 0.315)):
            result = run_retraining()

    assert result["deployed"] is False
    mock_deploy.assert_not_called()


@patch("ml.ct_cm.retrain.publish_retrain_metrics")
@patch("ml.ct_cm.retrain.save_retrain_history")
@patch("ml.ct_cm.retrain.write_retrain_event")
@patch("ml.ct_cm.retrain.deploy_model")
@patch("ml.ct_cm.retrain.get_production_val_loss")
@patch("ml.ct_cm.retrain.build_embedding_lookup")
@patch("ml.ct_cm.retrain.fetch_all_embeddings")
@patch("ml.ct_cm.retrain.get_client")
@patch("ml.ct_cm.retrain.firestore.Client")
def test_run_retraining_result_has_required_keys(
    mock_fs, mock_bq, mock_fetch_embeddings, mock_build_lookup,
    mock_prod_loss, mock_deploy, mock_write, mock_save, mock_publish
):
    """Result dict always contains all required keys."""
    mock_fs.return_value = MagicMock()
    mock_bq.return_value = MagicMock()
    mock_prod_loss.return_value = 0.5

    video_ids = [f"v{i}" for i in range(10)]
    mock_fetch_embeddings.return_value = _make_songs_df(10)
    mock_build_lookup.return_value = _make_embedding_lookup(video_ids)

    with patch("ml.ct_cm.retrain.fetch_real_sessions",
               return_value=[[{"video_id": vid, "liked_flag": 0}
                               for vid in video_ids]] * MIN_SESSIONS_REQUIRED):
        with patch("ml.ct_cm.retrain.train",
                   return_value=(MagicMock(), 0.3)):
            result = run_retraining()

    required_keys = {
        "status", "deployed", "new_val_loss",
        "production_val_loss", "improvement",
        "num_sessions", "num_samples", "timestamp", "reason",
    }
    assert required_keys.issubset(result.keys())


@patch("ml.ct_cm.retrain.publish_retrain_metrics")
@patch("ml.ct_cm.retrain.save_retrain_history")
@patch("ml.ct_cm.retrain.write_retrain_event")
@patch("ml.ct_cm.retrain.deploy_model")
@patch("ml.ct_cm.retrain.get_production_val_loss")
@patch("ml.ct_cm.retrain.build_embedding_lookup")
@patch("ml.ct_cm.retrain.fetch_all_embeddings")
@patch("ml.ct_cm.retrain.get_client")
@patch("ml.ct_cm.retrain.firestore.Client")
def test_run_retraining_always_publishes_metrics(
    mock_fs, mock_bq, mock_fetch_embeddings, mock_build_lookup,
    mock_prod_loss, mock_deploy, mock_write, mock_save, mock_publish
):
    """Cloud Monitoring metrics always published after retraining."""
    mock_fs.return_value = MagicMock()
    mock_bq.return_value = MagicMock()
    mock_prod_loss.return_value = 0.5

    video_ids = [f"v{i}" for i in range(10)]
    mock_fetch_embeddings.return_value = _make_songs_df(10)
    mock_build_lookup.return_value = _make_embedding_lookup(video_ids)

    with patch("ml.ct_cm.retrain.fetch_real_sessions",
               return_value=[[{"video_id": vid, "liked_flag": 0}
                               for vid in video_ids]] * MIN_SESSIONS_REQUIRED):
        with patch("ml.ct_cm.retrain.train",
                   return_value=(MagicMock(), 0.3)):
            run_retraining()

    mock_publish.assert_called_once()
    mock_write.assert_called_once()
    mock_save.assert_called_once()