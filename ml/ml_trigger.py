"""
ML Trigger — entry point for the ML recommendation system.

Subscribes to SESSION_READY_TOPIC from Pub/Sub.
When batch pipeline finishes, this fires and:
1. Runs ML preprocessing (preprocess + embeddings)
2. Initializes session (loads BigQuery + GRU)
3. Generates first batch of 30 recommendations immediately
4. Watches Firestore songs_played_count
5. Regenerates next batch every REFRESH_THRESHOLD songs played

Queue strategy:
  Batch 1 (30 songs) → generated at session start
  Batch 2 (30 songs) → generated after song 10 plays (background)
  Batch 3 (30 songs) → generated after song 20 plays (background)
  User always has 20-30 songs ahead → zero waiting time
"""

import base64
import json
import logging
import os
import threading
import time

import functions_framework

from ml.preprocessing.preprocess_songs import preprocess_for_session
from ml.feature_engineering.generate_song_embeddings import (
    generate_embeddings_for_session,
)
from ml.recommendation.main import initialize_session, recommend_next_song
from ml.recommendation.firestore_client import (
    get_songs_played_count,
    update_last_refreshed_at,
    is_session_active,
)
from ml.recommendation.config import (
    PROJECT_ID,
    DATABASE_ID,
    BUCKET,
    RECOMMENDATION_BATCH_SIZE,
    REFRESH_THRESHOLD,
    TOP_N,
)
# ── Monitoring imports ────────────────────────────────────────────────────────
from ml.recommendation.firestore_monitoring import MonitoringWriter   # ADD
from ml.evaluation.model_bias import evaluate as evaluate_bias        # ADD

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Recommendation batch generator ───────────────────────────────────────────
def _generate_and_write_batch(state, batch_number: int, monitoring_writer: MonitoringWriter):  # ADD monitoring_writer param
    """
    Runs one recommendation cycle and writes results to Firestore queue.

    Each batch generates RECOMMENDATION_BATCH_SIZE (30) songs.
    TOP_N and RECOMMENDATION_BATCH_SIZE are both 30 in config.py —
    no override needed.

    Writes to: sessions/{session_id}/recommendations/{rank}

    monitoring_writer is used to write a bias snapshot after each batch.
    The rec snapshot (per-song CBF/CF/GRU scores) is written inside
    recommend_next_song() in ml/recommendation/main.py where the full
    scored list is available.
    """
    logger.info(
        f"Generating batch {batch_number} "
        f"for session {state.session_id}..."
    )

    top_song = recommend_next_song(state)

    if not top_song:
        logger.warning(f"Batch {batch_number} returned no recommendations.")
        return

    logger.info(
        f"Batch {batch_number} complete. "
        f"Top song: '{top_song.get('track_title', 'unknown')}'"
    )

    # ── Bias snapshot after every batch ──────────────────────────────────────
    # recommend_next_song() writes the rec snapshot (CBF/CF/GRU scores) from
    # inside main.py where the full scored list exists. Bias evaluation runs
    # here because state.catalog_df (the BigQuery catalog) is accessible and
    # we want bias checked against every batch, not just the first.
    try:
        # state.last_batch_songs is set by recommend_next_song() in main.py
        # It holds the list of dicts for the 30 songs just recommended.
        # See Step 2 (main.py wiring) for where this is assigned.
        last_batch = getattr(state, "last_batch_songs", None)
        if last_batch and hasattr(state, "catalog_df"):
            bias_result = evaluate_bias(
                recommendations=last_batch,
                catalog_df=state.catalog_df,
                score_column="final_score",
            )
            monitoring_writer.write_bias_snapshot(bias_result)
            if bias_result["overall_bias_detected"]:
                logger.warning(
                    "Bias detected in batch %d for session %s: %s",
                    batch_number,
                    state.session_id,
                    bias_result["mitigation_suggestions"],
                )
        else:
            logger.debug(
                "Skipping bias snapshot — state.last_batch_songs not set yet. "
                "Wire recommend_next_song() in main.py to set it."
            )
    except Exception as exc:
        # Bias evaluation must never crash a live session
        logger.error(
            "Bias snapshot write failed (non-critical) batch %d: %s",
            batch_number, exc,
        )


# ── Refresh watcher ───────────────────────────────────────────────────────────
def _watch_and_refresh(state, monitoring_writer: MonitoringWriter):  # ADD monitoring_writer param
    """
    Runs in a background thread for the duration of the session.
    Polls Firestore songs_played_count every 10 seconds.
    Triggers a new recommendation batch every REFRESH_THRESHOLD songs.

    Polling every 10s ensures refresh happens well before
    the current batch runs out (batch=30 songs, refresh every 10).
    """
    session_id   = state.session_id
    db           = state.db
    batch_number = 2    # batch 1 already generated at session start

    logger.info(
        f"Refresh watcher started for session {session_id}. "
        f"Refreshing every {REFRESH_THRESHOLD} songs."
    )

    while True:
        time.sleep(10)   # poll every 10 seconds

        # stop if session ended
        if not is_session_active(db, session_id):
            logger.info(
                f"Session {session_id} ended. "
                f"Stopping refresh watcher."
            )
            break

        songs_played   = get_songs_played_count(db, session_id)
        last_refreshed = _get_last_refreshed_at(db, session_id)
        next_threshold = last_refreshed + REFRESH_THRESHOLD

        if songs_played >= next_threshold:
            logger.info(
                f"Refresh triggered — "
                f"songs played: {songs_played}, "
                f"threshold: {next_threshold}"
            )
            update_last_refreshed_at(db, session_id, songs_played)
            _generate_and_write_batch(state, batch_number, monitoring_writer)  # PASS monitoring_writer
            batch_number += 1


def _get_last_refreshed_at(db, session_id: str) -> int:
    """
    Reads last_refreshed_at from Firestore session metadata.
    Used to compute the next refresh threshold.
    """
    try:
        doc = (
            db.collection("sessions")
            .document(session_id)
            .collection("metadata")
            .document("state")
            .get()
        )
        if doc.exists:
            return doc.to_dict().get("last_refreshed_at", 0)
        return 0
    except Exception as e:
        logger.error(f"Failed to read last_refreshed_at: {e}")
        return 0


# ── ML preprocessing ──────────────────────────────────────────────────────────
def _run_ml_preprocessing(session_id: str, bucket: str):
    """
    Runs preprocessing and embedding generation for the session.
    Called once at session start before recommendations are generated.

    Steps:
    1. preprocess_for_session()
       → reads mitigated.csv from GCS
       → cleans, normalizes, computes popularity_score
       → writes preprocessed.csv to GCS

    2. generate_embeddings_for_session()
       → reads preprocessed.csv from GCS
       → generates 386-dim embeddings for new songs
       → uploads to BigQuery song catalog
    """
    logger.info(f"Running ML preprocessing for session {session_id}...")

    preprocessed_path = preprocess_for_session(session_id, bucket)
    logger.info(f"Preprocessing complete: {preprocessed_path}")

    new_songs = generate_embeddings_for_session(session_id, bucket)
    logger.info(
        f"Embeddings complete: {new_songs} new songs added to BigQuery"
    )


# ── Main session runner ───────────────────────────────────────────────────────
def _run_ml_session(session_id: str):
    """
    Full ML session flow:
    1. Preprocess + generate embeddings
    2. Initialize session state
    3. Generate first batch immediately (songs 1-30)
    4. Start background refresh watcher
    """
    bucket = os.environ.get("BUCKET", BUCKET)

    try:
        # step 1: ML preprocessing
        _run_ml_preprocessing(session_id, bucket)

        # step 2: initialize session
        # loads BigQuery catalog, GRU model, session users
        state = initialize_session(session_id)

        # ── Initialise MonitoringWriter once per session ──────────────────
        # state.db is the Firestore client set up by initialize_session().
        # One writer instance is shared across all batches for this session.
        monitoring_writer        = MonitoringWriter(state.db, session_id)
        state.monitoring_writer  = monitoring_writer   # wire into state so recommend_next_song can call write_rec_snapshot
        logger.info("MonitoringWriter initialised for session %s", session_id)

        # step 3: generate first batch immediately
        logger.info(
            "Generating initial recommendation batch (songs 1-30)..."
        )
        _generate_and_write_batch(state, batch_number=1, monitoring_writer=monitoring_writer)  # PASS monitoring_writer

        # mark first refresh done at song count 0
        update_last_refreshed_at(state.db, session_id, 0)

        # step 4: start background refresh watcher
        watcher = threading.Thread(
            target=_watch_and_refresh,
            args=(state, monitoring_writer),   # PASS monitoring_writer
            daemon=True
        )
        watcher.start()
        logger.info(
            f"ML session {session_id} running. "
            f"Refresh watcher active."
        )

        # keep alive until session ends
        watcher.join()

    except Exception as e:
        logger.error(
            f"ML session {session_id} failed: {e}",
            exc_info=True
        )


# ── Cloud Function trigger ────────────────────────────────────────────────────
@functions_framework.cloud_event
def ml_session_trigger(cloud_event):
    """
    Cloud Function triggered by SESSION_READY_TOPIC Pub/Sub message.
    Fires automatically when batch pipeline completes.

    Pub/Sub message format:
    { "session_id": "abc-123" }
    """
    try:
        data       = cloud_event.data["message"]["data"]
        payload    = json.loads(base64.b64decode(data).decode("utf-8"))
        session_id = payload["session_id"]
    except Exception as e:
        logger.error(f"Failed to decode Pub/Sub message: {e}")
        return

    logger.info(f"ML trigger received session_id: {session_id}")

    # run in background thread so Cloud Function returns immediately
    thread = threading.Thread(
        target=_run_ml_session,
        args=(session_id,),
        daemon=True
    )
    thread.start()


# ── Local testing ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m ml.ml_trigger <session_id>")
        sys.exit(1)

    session_id = sys.argv[1]
    _run_ml_session(session_id)
