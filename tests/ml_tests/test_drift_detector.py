"""
Unit tests for ml.ct_cm.drift_detector
All tests run without any GCP dependencies.
Firestore and Cloud Monitoring clients are mocked.
"""
import math
import pytest
from unittest.mock import MagicMock, patch
from ml.ct_cm.drift_detector import (
    kl_divergence,
    get_production_genre_distribution,
    run_drift_detection,
    DRIFT_THRESHOLD_KL,
    TRAINING_GENRE_DISTRIBUTION,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

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
          .collection.return_value.limit.return_value \
          .stream.return_value = evt_docs

        session_docs.append(session_doc)

    db.collection.return_value.stream.return_value = session_docs
    return db


# ── KL Divergence Tests ───────────────────────────────────────────────────────

def test_kl_divergence_identical_distributions():
    """KL(P || P) = 0 for identical distributions."""
    p = {"chill": 0.5, "pop": 0.3, "rock": 0.2}
    result = kl_divergence(p, p)
    assert result == pytest.approx(0.0, abs=1e-4)


def test_kl_divergence_completely_different():
    """KL divergence is high when distributions don't overlap."""
    p = {"chill": 1.0}
    q = {"pop": 1.0}
    result = kl_divergence(p, q)
    assert result > 1.0


def test_kl_divergence_partial_overlap():
    """KL divergence is between 0 and 1 for partial overlap."""
    p = {"chill": 0.6, "pop": 0.4}
    q = {"chill": 0.4, "pop": 0.6}
    result = kl_divergence(p, q)
    assert 0.0 < result < 1.0


def test_kl_divergence_handles_zero_in_q():
    """KL divergence handles zero values in Q with epsilon smoothing."""
    p = {"chill": 0.7, "pop": 0.3}
    q = {"chill": 1.0}  # pop missing from q
    result = kl_divergence(p, q)
    assert result > 0.0
    assert not math.isnan(result)
    assert not math.isinf(result)


def test_kl_divergence_asymmetric():
    """KL divergence is asymmetric: KL(P||Q) != KL(Q||P)."""
    p = {"chill": 0.8, "pop": 0.2}
    q = {"chill": 0.2, "pop": 0.8}
    kl_pq = kl_divergence(p, q)
    kl_qp = kl_divergence(q, p)
    assert kl_pq != pytest.approx(kl_qp, abs=1e-4)


def test_kl_divergence_returns_float():
    """KL divergence always returns a float."""
    p = {"chill": 0.5, "pop": 0.5}
    q = {"chill": 0.3, "pop": 0.7}
    result = kl_divergence(p, q)
    assert isinstance(result, float)


def test_kl_divergence_rounded_to_4_decimals():
    """KL divergence is rounded to 4 decimal places."""
    p = {"chill": 0.5, "pop": 0.5}
    q = {"chill": 0.3, "pop": 0.7}
    result = kl_divergence(p, q)
    assert result == round(result, 4)


# ── Genre Distribution Tests ──────────────────────────────────────────────────

def test_get_production_genre_distribution_empty_db():
    """Returns empty dict when no sessions in Firestore."""
    db = MagicMock()
    db.collection.return_value.stream.return_value = []
    result = get_production_genre_distribution(db)
    assert result == {}


def test_get_production_genre_distribution_filters_active_sessions():
    """Only includes ended/done sessions."""
    db = _make_mock_db([
        ({"status": "active", "session_id": "s1"}, [
            {"genre": "chill", "video_id": "v1"},
        ]),
    ])
    result = get_production_genre_distribution(db)
    assert result == {}


def test_get_production_genre_distribution_single_genre():
    """Returns correct distribution for single genre."""
    db = _make_mock_db([
        ({"status": "ended", "session_id": "s1"}, [
            {"genre": "chill", "video_id": "v1"},
            {"genre": "chill", "video_id": "v2"},
            {"genre": "chill", "video_id": "v3"},
        ]),
    ])
    result = get_production_genre_distribution(db)
    assert "chill" in result
    assert result["chill"] == pytest.approx(1.0, abs=1e-4)


def test_get_production_genre_distribution_multiple_genres():
    """Returns normalized distribution across multiple genres."""
    db = _make_mock_db([
        ({"status": "ended", "session_id": "s1"}, [
            {"genre": "chill", "video_id": "v1"},
            {"genre": "chill", "video_id": "v2"},
            {"genre": "pop", "video_id": "v3"},
            {"genre": "pop", "video_id": "v4"},
        ]),
    ])
    result = get_production_genre_distribution(db)
    assert result["chill"] == pytest.approx(0.5, abs=1e-4)
    assert result["pop"] == pytest.approx(0.5, abs=1e-4)


def test_get_production_genre_distribution_normalizes_to_one():
    """Distribution always sums to 1.0."""
    db = _make_mock_db([
        ({"status": "ended", "session_id": "s1"}, [
            {"genre": "chill",   "video_id": "v1"},
            {"genre": "pop",     "video_id": "v2"},
            {"genre": "rock",    "video_id": "v3"},
            {"genre": "hip-hop", "video_id": "v4"},
        ]),
    ])
    result = get_production_genre_distribution(db)
    assert sum(result.values()) == pytest.approx(1.0, abs=1e-4)


def test_get_production_genre_distribution_handles_missing_genre():
    """Songs with no genre field default to 'other'."""
    db = _make_mock_db([
        ({"status": "ended", "session_id": "s1"}, [
            {"video_id": "v1"},  # no genre field
            {"genre": "",        "video_id": "v2"},  # empty genre
        ]),
    ])
    result = get_production_genre_distribution(db)
    assert "other" in result


# ── Run Drift Detection Tests ─────────────────────────────────────────────────

@patch("ml.ct_cm.drift_detector.publish_drift_metrics")
@patch("ml.ct_cm.drift_detector.write_drift_event")
@patch("ml.ct_cm.drift_detector.firestore.Client")
def test_run_drift_detection_no_data(mock_fs, mock_write, mock_publish):
    """Returns skipped status when no production data available."""
    mock_db = MagicMock()
    mock_db.collection.return_value.stream.return_value = []
    mock_fs.return_value = mock_db

    result = run_drift_detection()

    assert result["status"] == "skipped"
    assert result["drift_detected"] is False
    assert result["kl_divergence"] == 0.0
    mock_write.assert_called_once()
    mock_publish.assert_not_called()


@patch("ml.ct_cm.drift_detector.publish_drift_metrics")
@patch("ml.ct_cm.drift_detector.write_drift_event")
@patch("ml.ct_cm.drift_detector.firestore.Client")
def test_run_drift_detection_no_drift(mock_fs, mock_write, mock_publish):
    """Returns completed status with drift_detected=False when KL below threshold."""
    mock_db = _make_mock_db([
        ({"status": "ended", "session_id": "s1"}, [
            {"genre": "chill",    "video_id": "v1"},
            {"genre": "energize", "video_id": "v2"},
            {"genre": "commute",  "video_id": "v3"},
        ]),
    ])
    mock_fs.return_value = mock_db

    # mock production distribution close to training distribution
    with patch("ml.ct_cm.drift_detector.get_production_genre_distribution",
               return_value=TRAINING_GENRE_DISTRIBUTION):
        result = run_drift_detection()

    assert result["status"] == "completed"
    assert result["drift_detected"] is False
    assert result["kl_divergence"] == pytest.approx(0.0, abs=0.01)
    assert result["recommendation"] == "No retraining needed"


@patch("ml.ct_cm.drift_detector.publish_drift_metrics")
@patch("ml.ct_cm.drift_detector.write_drift_event")
@patch("ml.ct_cm.drift_detector.firestore.Client")
def test_run_drift_detection_drift_detected(mock_fs, mock_write, mock_publish):
    """Returns drift_detected=True when KL exceeds threshold."""
    mock_fs.return_value = MagicMock()

    # completely different distribution from training
    drifted_dist = {"hiphoprap": 0.9, "drill": 0.1}

    with patch("ml.ct_cm.drift_detector.get_production_genre_distribution",
               return_value=drifted_dist):
        result = run_drift_detection()

    assert result["status"] == "completed"
    assert result["drift_detected"] is True
    assert result["kl_divergence"] > DRIFT_THRESHOLD_KL
    assert result["recommendation"] == "Trigger retraining"
    mock_publish.assert_called_once()
    mock_write.assert_called_once()


@patch("ml.ct_cm.drift_detector.publish_drift_metrics")
@patch("ml.ct_cm.drift_detector.write_drift_event")
@patch("ml.ct_cm.drift_detector.firestore.Client")
def test_run_drift_detection_returns_top_drifted_genres(mock_fs, mock_write, mock_publish):
    """Result includes top drifted genres sorted by deviation."""
    mock_fs.return_value = MagicMock()

    drifted_dist = {"hiphoprap": 0.8, "chill": 0.2}

    with patch("ml.ct_cm.drift_detector.get_production_genre_distribution",
               return_value=drifted_dist):
        result = run_drift_detection()

    assert "top_drifted_genres" in result
    if result["top_drifted_genres"]:
        deviations = [g["deviation"] for g in result["top_drifted_genres"]]
        assert deviations == sorted(deviations, reverse=True)


@patch("ml.ct_cm.drift_detector.publish_drift_metrics")
@patch("ml.ct_cm.drift_detector.write_drift_event")
@patch("ml.ct_cm.drift_detector.firestore.Client")
def test_run_drift_detection_result_has_required_keys(mock_fs, mock_write, mock_publish):
    """Result dict always contains all required keys."""
    mock_fs.return_value = MagicMock()

    with patch("ml.ct_cm.drift_detector.get_production_genre_distribution",
               return_value={"chill": 1.0}):
        result = run_drift_detection()

    required_keys = {
        "status", "drift_detected", "kl_divergence",
        "threshold", "timestamp", "recommendation",
    }
    assert required_keys.issubset(result.keys())


@patch("ml.ct_cm.drift_detector.publish_drift_metrics")
@patch("ml.ct_cm.drift_detector.write_drift_event")
@patch("ml.ct_cm.drift_detector.firestore.Client")
def test_run_drift_detection_always_writes_event(mock_fs, mock_write, mock_publish):
    """Drift event is always written to Firestore regardless of result."""
    mock_db = MagicMock()
    mock_db.collection.return_value.stream.return_value = []
    mock_fs.return_value = mock_db

    run_drift_detection()
    mock_write.assert_called_once()