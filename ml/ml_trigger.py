"""
ML Trigger — entry point for the ML recommendation system.

Triggered by server.py /ml endpoint when SESSION_READY_TOPIC fires.
server.py receives the Pub/Sub message, decodes session_id,
and calls _run_ml_session() directly in a background thread.

Flow:
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

import logging
import os
import threading
import time

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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Recommendation batch generator ───────────────────────────────────────────
def _generate_and_write_batch(state, batch_number: int):
    """
    Runs one recommendation cycle and writes results to Firestore queue.

    Each batch generates RECOMMENDATION_BATCH_SIZE songs.
    Writes to: sessions/{session_id}/recommendations/{rank}

    Batch 1: ranks 1-30
    Batch 2: ranks 31-60
    Batch 3: ranks 61-90
    """
    logger.info(
        f"Generating batch {batch_number} "
        f"for session {state.session_id}..."
    )

    # temporarily override TOP_N to match batch size
    import ml.recommendation.config as cfg
    original_top_n = cfg.TOP_N
    cfg.TOP_N      = RECOMMENDATION_BATCH_SIZE

    try:
        top_song = recommend_next_song(state)
    finally:
        cfg.TOP_N = original_top_n

    if not top_song:
        logger.warning(f"Batch {batch_number} returned no recommendations.")
        return

    logger.info(
        f"Batch {batch_number} complete. "
        f"Top song: '{top_song.get('track_title', 'unknown')}'"
    )


# ── Refresh watcher ───────────────────────────────────────────────────────────
def _watch_and_refresh(state):
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
            _generate_and_write_batch(state, batch_number)
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

        # step 3: generate first batch immediately
        logger.info(
            "Generating initial recommendation batch (songs 1-30)..."
        )
        _generate_and_write_batch(state, batch_number=1)

        # mark first refresh done at song count 0
        update_last_refreshed_at(state.db, session_id, 0)

        # step 4: start background refresh watcher
        watcher = threading.Thread(
            target=_watch_and_refresh,
            args=(state,),
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


# ── NOTE ──────────────────────────────────────────────────────────────────────
# _run_ml_session is called by server.py /ml endpoint.
# server.py receives the Pub/Sub push message, decodes session_id,
# and calls _run_ml_session() in a background thread.
# There is no Cloud Function entry point — Cloud Run handles it directly.


# ── Local testing ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m ml.ml_trigger <session_id>")
        sys.exit(1)

    session_id = sys.argv[1]
    _run_ml_session(session_id)