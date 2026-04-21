"""
ml/ct_cm/retrain.py
-------------------
Continuous Training (CT) pipeline for the GRU sequential model.

Triggered by:
  - Cloud Scheduler every 3 days via POST /retrain
  - Manually via: python -m ml.ct_cm.retrain

Flow:
  1. Fetch real session sequences from Firestore song_events
  2. Build training dataset from real play sequences
  3. Retrain GRU using same architecture as synthetic training
  4. Compare new model val_loss vs current production model
  5. If improved → upload to GCS, replace production model
  6. Log results to Cloud Monitoring and Firestore

Retraining threshold:
  New model deployed only if val_loss improves by > IMPROVEMENT_THRESHOLD
  This prevents regression from noisy retraining runs.
"""

import io
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import torch
from google.cloud import firestore, storage

from ml.recommendation.config import (
    PROJECT_ID,
    DATABASE_ID,
    GRU_EMBEDDING_DIM,
    GRU_HIDDEN_DIM,
    GRU_NUM_LAYERS,
    GRU_MODEL_PATH,
)
from ml.gru.gru_model import build_model
from ml.gru.train_gru import train, SessionDataset
from ml.recommendation.bigquery_client import get_client, fetch_all_embeddings
from ml.gru.gru_inference import build_embedding_lookup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
IMPROVEMENT_THRESHOLD  = 0.01
MIN_SESSIONS_REQUIRED  = 50
MIN_SONGS_PER_SESSION  = 5
LOOKBACK_DAYS          = 30
GCS_BUCKET             = os.environ.get("ML_MODELS_BUCKET", "auxless-music-recommender-ml-models")
MODEL_BLOB             = "models/gru_model.pt"
MODEL_BACKUP_BLOB      = "models/gru_model_backup_{ts}.pt"
RETRAIN_HISTORY_BLOB   = "models/retrain_history.json"
MAX_SEQ_LEN            = 15


# ── Firestore sequence fetcher ────────────────────────────────────────────────
def fetch_real_sessions(db: firestore.Client, lookback_days: int = LOOKBACK_DAYS) -> list:
    """
    Fetches real play sequences from Firestore song_events subcollections.

    Returns list of sequences:
    [
        [
            {"video_id": "abc", "liked_flag": 0},
            {"video_id": "xyz", "liked_flag": 1},
            ...
        ],
        ...
    ]
    """
    sessions = []

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
                .order_by("play_order")
                .stream()
            )

            if len(song_events) < MIN_SONGS_PER_SESSION:
                continue

            sequence = []
            for evt in song_events:
                e = evt.to_dict()
                vid = e.get("video_id", "").strip()
                if not vid:
                    continue
                sequence.append({
                    "video_id":   vid,
                    "liked_flag": int(e.get("liked_flag", 0)),
                })

            if len(sequence) >= MIN_SONGS_PER_SESSION:
                sessions.append(sequence)

    except Exception as e:
        logger.error(f"Failed to fetch real sessions: {e}")

    logger.info(f"Fetched {len(sessions)} real sessions from Firestore")
    return sessions


# ── Build training samples from real sequences ────────────────────────────────
def build_training_samples(
    sessions: list,
    embedding_lookup: dict,
) -> list:
    """
    Converts real play sequences into (input_tensor, target_embedding) pairs.
    Same format as synthetic training — compatible with train().

    For each session of length N, generates N-1 subsequence samples:
      [s1] → s2_embedding
      [s1, s2] → s3_embedding
      ...
    """
    samples = []

    for sequence in sessions:
        valid_seq = [
            evt for evt in sequence
            if evt["video_id"] in embedding_lookup
        ]

        if len(valid_seq) < 2:
            continue

        for i in range(1, len(valid_seq)):
            history = valid_seq[:i]
            target  = valid_seq[i]

            seq_tensors = []
            for evt in history:
                emb  = embedding_lookup[evt["video_id"]]
                flag = np.array([evt["liked_flag"]], dtype=np.float32)
                seq_tensors.append(np.concatenate([emb, flag]))

            while len(seq_tensors) < MAX_SEQ_LEN:
                seq_tensors.insert(0, np.zeros(GRU_EMBEDDING_DIM + 1, dtype=np.float32))
            seq_tensors = seq_tensors[-MAX_SEQ_LEN:]

            input_tensor  = torch.tensor(np.stack(seq_tensors), dtype=torch.float32)
            target_tensor = torch.tensor(
                embedding_lookup[target["video_id"]], dtype=torch.float32
            )

            samples.append((input_tensor, target_tensor))

    logger.info(f"Built {len(samples)} training samples from real sessions")
    return samples


# ── Load current production model val_loss ────────────────────────────────────
def get_production_val_loss() -> float:
    """
    Reads the last recorded val_loss from retrain_history.json in GCS.
    Returns inf if no history exists — forces first retrain to deploy.
    """
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(RETRAIN_HISTORY_BLOB)
        if blob.exists():
            history = json.loads(blob.download_as_text())
            return history.get("production_val_loss", float("inf"))
    except Exception as e:
        logger.warning(f"Could not read retrain history: {e}")
    return float("inf")


# ── Save retrain history ──────────────────────────────────────────────────────
def save_retrain_history(result: dict):
    """Persists retraining result to GCS for future comparisons."""
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        blob   = bucket.blob(RETRAIN_HISTORY_BLOB)
        blob.upload_from_string(json.dumps(result, indent=2))
        logger.info("Retrain history saved to GCS")
    except Exception as e:
        logger.error(f"Failed to save retrain history: {e}")


# ── Deploy new model to GCS ───────────────────────────────────────────────────
def deploy_model(model: torch.nn.Module, val_loss: float):
    """Backs up current production model then uploads new model."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    try:
        ts          = int(time.time())
        backup_blob = MODEL_BACKUP_BLOB.format(ts=ts)
        bucket.copy_blob(bucket.blob(MODEL_BLOB), bucket, backup_blob)
        logger.info(f"Current model backed up to {backup_blob}")
    except Exception as e:
        logger.warning(f"Could not backup current model: {e}")

    buffer = io.BytesIO()
    torch.save(model.state_dict(), buffer)
    buffer.seek(0)
    bucket.blob(MODEL_BLOB).upload_from_file(buffer)
    logger.info(f"New model deployed to gs://{GCS_BUCKET}/{MODEL_BLOB} (val_loss={val_loss:.4f})")


# ── Publish metrics to Cloud Monitoring ──────────────────────────────────────
def publish_retrain_metrics(val_loss: float, deployed: bool, num_sessions: int):
    """Writes retraining metrics to Cloud Monitoring custom metrics."""
    from google.cloud import monitoring_v3
    try:
        client     = monitoring_v3.MetricServiceClient()
        project    = f"projects/{PROJECT_ID}"
        now        = time.time()
        series     = monitoring_v3.TimeSeries()
        series.metric.type = "custom.googleapis.com/auxless/gru_val_loss"
        series.resource.type = "global"
        series.resource.labels["project_id"] = PROJECT_ID

        point = monitoring_v3.Point()
        point.value.double_value = val_loss
        point.interval.end_time.seconds = int(now)
        series.points = [point]

        client.create_time_series(name=project, time_series=[series])
        logger.info(f"Published val_loss={val_loss:.4f} to Cloud Monitoring")
    except Exception as e:
        logger.warning(f"Could not publish to Cloud Monitoring: {e}")


# ── Write retrain result to Firestore ────────────────────────────────────────
def write_retrain_event(db: firestore.Client, result: dict):
    """Writes retraining result to Firestore for dashboard visibility."""
    try:
        db.collection("retrain_events").add({
            **result,
            "created_at": firestore.SERVER_TIMESTAMP,
        })
        logger.info("Retrain event written to Firestore")
    except Exception as e:
        logger.error(f"Failed to write retrain event: {e}")


# ── Main retraining pipeline ──────────────────────────────────────────────────
def run_retraining() -> dict:
    """
    Full CT pipeline:
    1. Fetch real session sequences from Firestore
    2. Build training samples
    3. Retrain GRU using real sequences (90/10 train/val split)
    4. Compare val_loss vs production
    5. Deploy if improved
    6. Log results
    """
    logger.info("=" * 60)
    logger.info("Starting GRU retraining pipeline")
    logger.info("=" * 60)

    db = firestore.Client(project=PROJECT_ID, database=DATABASE_ID)

    # step 1: fetch real sessions
    sessions = fetch_real_sessions(db)

    if len(sessions) < MIN_SESSIONS_REQUIRED:
        result = {
            "status":       "skipped",
            "reason":       f"Only {len(sessions)} sessions available, need {MIN_SESSIONS_REQUIRED}",
            "num_sessions": len(sessions),
            "deployed":     False,
            "timestamp":    datetime.utcnow().isoformat(),
        }
        logger.info(f"Retraining skipped: {result['reason']}")
        write_retrain_event(db, result)
        return result

    # step 2: load embedding lookup
    logger.info("Loading BigQuery catalog for embedding lookup...")
    bq_client        = get_client()
    songs_df         = fetch_all_embeddings(bq_client)
    embedding_lookup = build_embedding_lookup(songs_df)
    logger.info(f"Loaded {len(embedding_lookup)} embeddings")

    # step 3: build training samples
    samples = build_training_samples(sessions, embedding_lookup)
    if not samples:
        result = {
            "status":       "skipped",
            "reason":       "No valid training samples after embedding lookup",
            "num_sessions": len(sessions),
            "deployed":     False,
            "timestamp":    datetime.utcnow().isoformat(),
        }
        write_retrain_event(db, result)
        return result

    # step 4: retrain GRU — split into train/val (90/10)
    split      = max(1, int(len(samples) * 0.9))
    train_seqs = samples[:split]
    val_seqs   = samples[split:] if samples[split:] else samples[:max(1, len(samples) // 10)]

    logger.info(f"Retraining GRU on {len(train_seqs)} train, {len(val_seqs)} val samples...")
    model, new_val_loss = train(
        train_seqs=train_seqs,
        val_seqs=val_seqs,
        embedding_lookup=embedding_lookup,
    )

    # step 5: compare vs production
    production_val_loss = get_production_val_loss()
    improvement         = production_val_loss - new_val_loss
    deployed            = improvement > IMPROVEMENT_THRESHOLD

    logger.info(f"Production val_loss: {production_val_loss:.4f}")
    logger.info(f"New val_loss:        {new_val_loss:.4f}")
    logger.info(f"Improvement:         {improvement:.4f}")
    logger.info(f"Deploy decision:     {'YES' if deployed else 'NO'}")

    if deployed:
        deploy_model(model, new_val_loss)

    # step 6: log results
    result = {
        "status":              "completed",
        "deployed":            deployed,
        "new_val_loss":        round(new_val_loss, 4),
        "production_val_loss": round(production_val_loss, 4),
        "improvement":         round(improvement, 4),
        "num_sessions":        len(sessions),
        "num_samples":         len(samples),
        "timestamp":           datetime.utcnow().isoformat(),
        "reason":              "Deployed - val_loss improved" if deployed
                               else f"Not deployed - improvement {improvement:.4f} below threshold {IMPROVEMENT_THRESHOLD}",
    }
    if deployed:
        result["production_val_loss"] = round(new_val_loss, 4)

    save_retrain_history(result)
    write_retrain_event(db, result)
    publish_retrain_metrics(new_val_loss, deployed, len(sessions))

    logger.info("=" * 60)
    logger.info(f"Retraining complete: {result['reason']}")
    logger.info("=" * 60)

    return result


# ── Local testing ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    result = run_retraining()
    print(json.dumps(result, indent=2))