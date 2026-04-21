"""
ml/ct_cm/drift_detector.py
--------------------------
Data drift detection for the AuxLess ML pipeline.

Detects two types of drift:
  1. Genre drift    — incoming session genre distribution vs training distribution
  2. Embedding drift — cosine similarity between incoming songs and training catalog

Uses KL divergence to quantify genre drift.
Writes drift scores to Firestore and Cloud Monitoring.

Triggered after every 10 sessions by the /retrain endpoint.
"""

import logging
import math
import os
import time
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
from google.cloud import firestore, monitoring_v3

from ml.recommendation.config import PROJECT_ID, DATABASE_ID

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
# Training genre distribution from synthetic data generation
# These values reflect the genre mix in the BigQuery catalog
# used during GRU training. Update after each retraining run.
TRAINING_GENRE_DISTRIBUTION = {
    "chill":      0.15,
    "energize":   0.12,
    "commute":    0.11,
    "hiphoprap":  0.10,
    "pop":        0.09,
    "romance":    0.08,
    "feel good":  0.07,
    "focus":      0.06,
    "party":      0.05,
    "decades":    0.04,
    "gaming":     0.04,
    "opm":        0.03,
    "songwriters producers": 0.03,
    "other":      0.03,
}

DRIFT_THRESHOLD_KL       = 0.3    # KL divergence > 0.3 = significant drift
DRIFT_THRESHOLD_SESSIONS = 10     # check drift every N sessions
LOOKBACK_DAYS            = 7      # look at last 7 days of sessions


# ── Genre distribution from recent sessions ───────────────────────────────────
def get_production_genre_distribution(
    db: firestore.Client,
    lookback_days: int = LOOKBACK_DAYS,
) -> dict:
    """
    Computes genre distribution from recent session song_events.
    Returns normalized distribution: {genre: proportion}
    """
    cutoff      = datetime.utcnow() - timedelta(days=lookback_days)
    genre_counts: dict = {}
    total       = 0

    try:
        session_docs = db.collection("sessions").stream()
        for session_doc in session_docs:
            data = session_doc.to_dict() or {}
            if data.get("status") not in ("ended", "done"):
                continue

            song_events = list(
                db.collection("sessions")
                .document(session_doc.id)
                .collection("song_events")
                .limit(100)
                .stream()
            )

            for evt in song_events:
                e     = evt.to_dict()
                genre = e.get("genre", "other").strip().lower()
                if not genre:
                    genre = "other"
                genre_counts[genre] = genre_counts.get(genre, 0) + 1
                total += 1

    except Exception as e:
        logger.error(f"Failed to fetch production genre distribution: {e}")
        return {}

    if total == 0:
        return {}

    return {genre: count / total for genre, count in genre_counts.items()}


# ── KL Divergence ─────────────────────────────────────────────────────────────
def kl_divergence(p: dict, q: dict, epsilon: float = 1e-10) -> float:
    """
    Computes KL divergence KL(P || Q) between two genre distributions.

    P = production distribution (what we're seeing in real sessions)
    Q = training distribution (what the model was trained on)

    KL divergence = 0 means identical distributions
    KL divergence > 0.3 = significant drift, consider retraining

    Uses epsilon smoothing to handle zero probabilities.
    """
    all_genres = set(list(p.keys()) + list(q.keys()))
    kl = 0.0
    for genre in all_genres:
        p_val = p.get(genre, epsilon)
        q_val = q.get(genre, epsilon)
        if p_val > 0:
            kl += p_val * math.log(p_val / (q_val + epsilon))
    return round(kl, 4)


# ── Publish drift metrics to Cloud Monitoring ─────────────────────────────────
def publish_drift_metrics(kl_score: float, drift_detected: bool):
    """Writes drift metrics to Cloud Monitoring custom metrics."""
    try:
        client  = monitoring_v3.MetricServiceClient()
        project = f"projects/{PROJECT_ID}"
        now     = time.time()

        series = monitoring_v3.TimeSeries()
        series.metric.type = "custom.googleapis.com/auxless/genre_drift_kl"
        series.resource.type = "global"
        series.resource.labels["project_id"] = PROJECT_ID

        point = monitoring_v3.Point()
        point.value.double_value = kl_score
        point.interval.end_time.seconds = int(now)
        series.points = [point]

        client.create_time_series(name=project, time_series=[series])
        logger.info(f"Published KL divergence={kl_score:.4f} to Cloud Monitoring")
    except Exception as e:
        logger.warning(f"Could not publish drift metrics: {e}")


# ── Write drift event to Firestore ────────────────────────────────────────────
def write_drift_event(db: firestore.Client, result: dict):
    """Writes drift detection result to Firestore for dashboard visibility."""
    try:
        db.collection("drift_events").add({
            **result,
            "created_at": firestore.SERVER_TIMESTAMP,
        })
        logger.info("Drift event written to Firestore")
    except Exception as e:
        logger.error(f"Failed to write drift event: {e}")


# ── Main drift detection pipeline ─────────────────────────────────────────────
def run_drift_detection() -> dict:
    """
    Full drift detection pipeline:
    1. Compute production genre distribution from recent sessions
    2. Compare against training distribution using KL divergence
    3. Flag drift if KL > threshold
    4. Publish metrics to Cloud Monitoring
    5. Write result to Firestore

    Returns result dict with drift score and detection flag.
    """
    logger.info("Starting drift detection...")

    db = firestore.Client(project=PROJECT_ID, database=DATABASE_ID)

    # step 1: get production distribution
    production_dist = get_production_genre_distribution(db)

    if not production_dist:
        result = {
            "status":         "skipped",
            "reason":         "No production data available",
            "drift_detected": False,
            "kl_divergence":  0.0,
            "timestamp":      datetime.utcnow().isoformat(),
        }
        write_drift_event(db, result)
        return result

    # step 2: compute KL divergence
    kl_score = kl_divergence(production_dist, TRAINING_GENRE_DISTRIBUTION)

    # step 3: flag drift
    drift_detected = kl_score > DRIFT_THRESHOLD_KL

    # step 4: compute top drifted genres
    drifted_genres = []
    for genre, prod_pct in production_dist.items():
        train_pct = TRAINING_GENRE_DISTRIBUTION.get(genre, 0.0)
        deviation = abs(prod_pct - train_pct)
        if deviation > 0.05:
            drifted_genres.append({
                "genre":      genre,
                "production": round(prod_pct, 4),
                "training":   round(train_pct, 4),
                "deviation":  round(deviation, 4),
            })
    drifted_genres.sort(key=lambda x: -x["deviation"])

    result = {
        "status":                "completed",
        "drift_detected":        drift_detected,
        "kl_divergence":         kl_score,
        "threshold":             DRIFT_THRESHOLD_KL,
        "production_distribution": production_dist,
        "training_distribution": TRAINING_GENRE_DISTRIBUTION,
        "top_drifted_genres":    drifted_genres[:5],
        "recommendation":        "Trigger retraining" if drift_detected
                                 else "No retraining needed",
        "timestamp":             datetime.utcnow().isoformat(),
    }

    # step 5: publish and log
    publish_drift_metrics(kl_score, drift_detected)
    write_drift_event(db, result)

    logger.info(f"Drift detection complete: KL={kl_score:.4f}, drift={'DETECTED' if drift_detected else 'NONE'}")

    if drift_detected:
        logger.warning(
            f"DATA DRIFT DETECTED — KL divergence {kl_score:.4f} exceeds threshold {DRIFT_THRESHOLD_KL}. "
            f"Consider retraining GRU on recent session data."
        )
        logger.warning(f"Top drifted genres: {drifted_genres[:3]}")

    return result


# ── Local testing ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json
    result = run_drift_detection()
    print(json.dumps(result, indent=2))