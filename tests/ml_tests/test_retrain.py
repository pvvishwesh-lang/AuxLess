"""
Unit tests for ml.ct_cm.retrain
All tests run without any GCP dependencies.
Firestore, GCS, and Cloud Monitoring clients are mocked.
"""
import io
import json
import numpy as np
import pandas as pd
import pytest
import torch
from unittest.mock import MagicMock, patch, call
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
    """Creates a fake embedding lookup with random embeddings."""
    rng = np.random.default_rng(42)
    return {
        vid: rng.random(DIM).astype(np.float32)
        for vid in video_ids
    }


def _make_mock_db(sessions: list) -> MagicMock:
    """
    Builds a mock Firestore client.
    sessions: list of (session_data, song_events) tuples
    """
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


# ── fetch_real_sessions Tests ─────────────────────────────────────────────────

def test_fetch_real_sessions_empty_db():
    """Returns empty list when no sessions in Firestore."""
    db = MagicMock()
    db.collection.return_value.stream.return_value = []
    result = fetch_real_sessions(db)
    assert result == []


def test_fetch_real_sessions_filters_active_sessions():
    """Only includes ended/done sessions."""
    db = _make_mock_db([
        ({"status": "active", "session_id": "s1"}, [
            {"video_id": f"v{i}", "liked_flag": 0} for i in range(10)
        ]),
    ])
    result = fetch_real_sessions(db)
    assert result == []


def test_fetch_real_sessions_filters_short_sessions():
    """Skips sessions with fewer than MIN_SONGS_PER_SESSION songs."""
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
    """Includes sessions with enough songs and ended status."""
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
    """Includes sessions with 'done' status."""
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
    """Skips events with empty video_id."""
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
    # empty video_id filtered out → 5 valid events >= MIN_SONGS_PER_SESSION
    assert len(result) == 1
    assert all(e["video_id"] != "" for e in result[0])


def test_fetch_real_sessions_returns_correct_structure():
    """Each session is a list of dicts with video_id and liked_flag."""
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
    """Fetches multiple valid sessions."""
    events = [
        {"video_id": f"v{i}", "liked_flag": 0}
        for i in range(MIN_SONGS_PER_SESSION + 1)
    ]
    db = _make_mock_db([
        ({"status": "ended", "session_id": "s1"}, events),
        ({"status": "ended", "session_id": "s2"}, events),
        ({"status": "active", "session_id": "s3"}, events),  # filtered
    ])
    result = fetch_real_sessions(db)
    assert len(result) == 2


# ── build_training_samples Tests ──────────────────────────────────────────────

def test_build_training_samples_empty_sessions():
    """Returns empty list for empty sessions."""
    result = build_training_samples([], {})
    assert result == []


def test_build_training_samples_filters_unknown_ids():
    """Skips songs not in embedding lookup."""
    sessions = [
        [
            {"video_id": "unknown_1", "liked_flag": 0},
            {"video_id": "unknown_2", "liked_flag": 0},
        ]
    ]
    result = build_training_samples(sessions, {})
    assert result == []


def test_build_training_samples_generates_subsequences():
    """For session of length N, generates N-1 samples."""
    video_ids = [f"v{i}" for i in range(5)]
    lookup    = _make_embedding_lookup(video_ids)
    sessions  = [[{"video_id": vid, "liked_flag": 0} for vid in video_ids]]

    result = build_training_samples(sessions, lookup)
    assert len(result) == 4  # N-1 = 5-1 = 4


def test_build_training_samples_returns_tensors():
    """Each sample is a (input_tensor, target_tensor) pair."""
    video_ids = [f"v{i}" for i in range(3)]
    lookup    = _make_embedding_lookup(video_ids)
    sessions  = [[{"video_id": vid, "liked_flag": 0} for vid in video_ids]]

    result = build_training_samples(sessions, lookup)
    assert len(result) > 0
    input_tensor, target_tensor = result[0]
    assert isinstance(input_tensor, torch.Tensor)
    assert isinstance(target_tensor, torch.Tensor)


def test_build_training_samples_input_shape():
    """Input tensor has shape (MAX_SEQ_LEN, DIM+1)."""
    video_ids = [f"v{i}" for i in range(5)]
    lookup    = _make_embedding_lookup(video_ids)
    sessions  = [[{"video_id": vid, "liked_flag": 0} for vid in video_ids]]

    result = build_training_samples(sessions, lookup)
    input_tensor, _ = result[0]
    assert input_tensor.shape == (15, DIM + 1)  # MAX_SEQ_LEN=15, DIM+1=387


def test_build_training_samples_target_shape():
    """Target tensor has shape (DIM,) = (386,)."""
    video_ids = [f"v{i}" for i in range(3)]
    lookup    = _make_embedding_lookup(video_ids)
    sessions  = [[{"video_id": vid, "liked_flag": 0} for vid in video_ids]]

    result = build_training_samples(sessions, lookup)
    _, target_tensor = result[0]
    assert target_tensor.shape == (DIM,)


def test_build_training_samples_liked_flag_in_input():
    """Liked flag is correctly appended to embedding in input tensor."""
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
    # last element of each non-padded row should be the liked_flag
    input_tensor, _ = result[0]
    # first sample: history=[v0], last position has v0's liked_flag=1
    assert input_tensor[-1, -1].item() == pytest.approx(1.0, abs=1e-4)


def test_build_training_samples_multiple_sessions():
    """Handles multiple sessions correctly."""
    video_ids = [f"v{i}" for i in range(4)]
    lookup    = _make_embedding_lookup(video_ids)
    sessions  = [
        [{"video_id": vid, "liked_flag": 0} for vid in video_ids],
        [{"video_id": vid, "liked_flag": 1} for vid in video_ids],
    ]
    result = build_training_samples(sessions, lookup)
    # 2 sessions × (4-1) = 6 samples
    assert len(result) == 6


# ── get_production_val_loss Tests ─────────────────────────────────────────────

@patch("ml.ct_cm.retrain.storage.Client")
def test_get_production_val_loss_no_history(mock_storage):
    """Returns inf when no retrain history exists in GCS."""
    mock_blob = MagicMock()
    mock_blob.exists.return_value = False
    mock_storage.return_value.bucket.return_value.blob.return_value = mock_blob

    result = get_production_val_loss()
    assert result == float("inf")


@patch("ml.ct_cm.retrain.storage.Client")
def test_get_production_val_loss_with_history(mock_storage):
    """Reads val_loss correctly from GCS history file."""
    history = {"production_val_loss": 0.3226, "timestamp": "2026-04-15"}
    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_blob.download_as_text.return_value = json.dumps(history)
    mock_storage.return_value.bucket.return_value.blob.return_value = mock_blob

    result = get_production_val_loss()
    assert result == pytest.approx(0.3226, abs=1e-4)


@patch("ml.ct_cm.retrain.storage.Client")
def test_get_production_val_loss_gcs_error(mock_storage):
    """Returns inf when GCS read fails."""
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
        for i in range(MIN_SESSIONS_REQUIRED - 1)  # one short
    ])
    mock_fs.return_value = mock_db

    result = run_retraining()

    assert result["status"] == "skipped"
    assert result["deployed"] is False
    assert "sessions available" in result["reason"]
    mock_write.assert_called_once()


@patch("ml.ct_cm.retrain.write_retrain_event")
@patch("ml.ct_cm.retrain.firestore.Client")
def test_run_retraining_skips_no_valid_samples(mock_fs, mock_write):
    """Skips retraining when no valid training samples after embedding lookup."""
    # enough sessions but all video_ids unknown to catalog
    sessions = [
        ({"status": "ended", "session_id": f"s{i}"}, [
            {"video_id": f"unknown_{j}", "liked_flag": 0}
            for j in range(MIN_SONGS_PER_SESSION + 1)
        ])
        for i in range(MIN_SESSIONS_REQUIRED)
    ]
    mock_db = _make_mock_db(sessions)
    mock_fs.return_value = mock_db

    with patch("ml.ct_cm.retrain.fetch_all_embeddings", return_value=_make_songs_df()):
        with patch("ml.ct_cm.retrain.build_embedding_lookup",
                   return_value={}):  # empty lookup
            result = run_retraining()

    assert result["status"] == "skipped"
    assert result["deployed"] is False


@patch("ml.ct_cm.retrain.publish_retrain_metrics")
@patch("ml.ct_cm.retrain.save_retrain_history")
@patch("ml.ct_cm.retrain.write_retrain_event")
@patch("ml.ct_cm.retrain.deploy_model")
@patch("ml.ct_cm.retrain.get_production_val_loss")
@patch("ml.ct_cm.retrain.firestore.Client")
def test_run_retraining_deploys_when_improved(
    mock_fs, mock_prod_loss, mock_deploy,
    mock_write, mock_save, mock_publish
):
    """Deploys new model when val_loss improves beyond threshold."""
    mock_fs.return_value = MagicMock()
    mock_prod_loss.return_value = 0.5  # current production val_loss

    with patch("ml.ct_cm.retrain.fetch_real_sessions",
               return_value=[[{"video_id": f"v{i}", "liked_flag": 0}
                               for i in range(10)]] * MIN_SESSIONS_REQUIRED):
        with patch("ml.ct_cm.retrain.fetch_all_embeddings",
                   return_value=_make_songs_df(10)):
            with patch("ml.ct_cm.retrain.build_embedding_lookup",
                       return_value=_make_embedding_lookup([f"v{i}" for i in range(10)])):
                mock_model = MagicMock()
                new_val_loss = 0.3  # improved by 0.2 > IMPROVEMENT_THRESHOLD
                with patch("ml.ct_cm.retrain.train",
                           return_value=(mock_model, new_val_loss)):
                    result = run_retraining()

    assert result["deployed"] is True
    assert result["status"] == "completed"
    assert result["new_val_loss"] == pytest.approx(0.3, abs=1e-4)
    mock_deploy.assert_called_once_with(mock_model, new_val_loss)


@patch("ml.ct_cm.retrain.publish_retrain_metrics")
@patch("ml.ct_cm.retrain.save_retrain_history")
@patch("ml.ct_cm.retrain.write_retrain_event")
@patch("ml.ct_cm.retrain.deploy_model")
@patch("ml.ct_cm.retrain.get_production_val_loss")
@patch("ml.ct_cm.retrain.firestore.Client")
def test_run_retraining_does_not_deploy_when_not_improved(
    mock_fs, mock_prod_loss, mock_deploy,
    mock_write, mock_save, mock_publish
):
    """Does not deploy when improvement is below threshold."""
    mock_fs.return_value = MagicMock()
    mock_prod_loss.return_value = 0.32  # current production

    with patch("ml.ct_cm.retrain.fetch_real_sessions",
               return_value=[[{"video_id": f"v{i}", "liked_flag": 0}
                               for i in range(10)]] * MIN_SESSIONS_REQUIRED):
        with patch("ml.ct_cm.retrain.fetch_all_embeddings",
                   return_value=_make_songs_df(10)):
            with patch("ml.ct_cm.retrain.build_embedding_lookup",
                       return_value=_make_embedding_lookup([f"v{i}" for i in range(10)])):
                mock_model = MagicMock()
                new_val_loss = 0.315  # improvement = 0.005 < IMPROVEMENT_THRESHOLD (0.01)
                with patch("ml.ct_cm.retrain.train",
                           return_value=(mock_model, new_val_loss)):
                    result = run_retraining()

    assert result["deployed"] is False
    mock_deploy.assert_not_called()


@patch("ml.ct_cm.retrain.publish_retrain_metrics")
@patch("ml.ct_cm.retrain.save_retrain_history")
@patch("ml.ct_cm.retrain.write_retrain_event")
@patch("ml.ct_cm.retrain.deploy_model")
@patch("ml.ct_cm.retrain.get_production_val_loss")
@patch("ml.ct_cm.retrain.firestore.Client")
def test_run_retraining_result_has_required_keys(
    mock_fs, mock_prod_loss, mock_deploy,
    mock_write, mock_save, mock_publish
):
    """Result dict always contains all required keys."""
    mock_fs.return_value = MagicMock()
    mock_prod_loss.return_value = 0.5

    with patch("ml.ct_cm.retrain.fetch_real_sessions",
               return_value=[[{"video_id": f"v{i}", "liked_flag": 0}
                               for i in range(10)]] * MIN_SESSIONS_REQUIRED):
        with patch("ml.ct_cm.retrain.fetch_all_embeddings",
                   return_value=_make_songs_df(10)):
            with patch("ml.ct_cm.retrain.build_embedding_lookup",
                       return_value=_make_embedding_lookup([f"v{i}" for i in range(10)])):
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
@patch("ml.ct_cm.retrain.firestore.Client")
def test_run_retraining_always_publishes_metrics(
    mock_fs, mock_prod_loss, mock_deploy,
    mock_write, mock_save, mock_publish
):
    """Cloud Monitoring metrics are always published after retraining."""
    mock_fs.return_value = MagicMock()
    mock_prod_loss.return_value = 0.5

    with patch("ml.ct_cm.retrain.fetch_real_sessions",
               return_value=[[{"video_id": f"v{i}", "liked_flag": 0}
                               for i in range(10)]] * MIN_SESSIONS_REQUIRED):
        with patch("ml.ct_cm.retrain.fetch_all_embeddings",
                   return_value=_make_songs_df(10)):
            with patch("ml.ct_cm.retrain.build_embedding_lookup",
                       return_value=_make_embedding_lookup([f"v{i}" for i in range(10)])):
                with patch("ml.ct_cm.retrain.train",
                           return_value=(MagicMock(), 0.3)):
                    run_retraining()

    mock_publish.assert_called_once()
    mock_write.assert_called_once()
    mock_save.assert_called_once()