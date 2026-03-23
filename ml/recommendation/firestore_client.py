"""
Firestore client for session-scoped data only.

User liked/disliked songs are stored in BigQuery (see bigquery_client.py).
Firestore handles only real-time session data:
  - Live track scores (updated by streaming pipeline)
  - Session play sequence (song_events subcollection)
  - Session user ids
  - Session metadata (songs_played_count, last_refreshed_at)
"""

import logging
from google.cloud import firestore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_db(project_id: str, database_id: str) -> firestore.Client:
    """
    Creates and returns a Firestore client.
    Credentials handled automatically by GCP (ADC).
    """
    return firestore.Client(project=project_id, database=database_id)


# ── Session track scores (real-time, for re-ranking + GRU) ───────────────────

def get_session_track_scores(db: firestore.Client, session_id: str) -> dict:
    """
    Fetches live track scores from the current session.
    Updated in real time by the streaming pipeline.
    Used for re-ranking recommendations after each song plays.

    Returns:
    {
        "video_id_a": {
            "score":         1.5,
            "like_count":    1,
            "skip_count":    1,
            "dislike_count": 0,
            "replay_count":  0,
            "last_updated":  timestamp
        },
        ...
    }
    """
    result = {}
    try:
        tracks_ref = (
            db.collection("sessions")
            .document(session_id)
            .collection("tracks")
            .stream()
        )
        for track in tracks_ref:
            data             = track.to_dict()
            result[track.id] = {
                "score":         data.get("score",         0.0),
                "like_count":    data.get("like_count",    0),
                "skip_count":    data.get("skip_count",    0),
                "dislike_count": data.get("dislike_count", 0),
                "replay_count":  data.get("replay_count",  0),
                "last_updated":  data.get("last_updated",  None),
            }
        logger.info(
            f"Fetched scores for {len(result)} tracks "
            f"in session {session_id}."
        )
    except Exception as e:
        logger.error(
            f"Failed to fetch track scores for session {session_id}: {e}"
        )
    return result


# ── Session play sequence (for GRU) ──────────────────────────────────────────

def get_session_play_sequence(db: firestore.Client, session_id: str) -> list:
    """
    Fetches the ordered sequence of songs played in the session.
    Used as input to the GRU model once 3+ songs have played.

    Returns list sorted by play_order:
    [
        { "video_id": "abc", "play_order": 1, "liked_flag": 1 },
        { "video_id": "xyz", "play_order": 2, "liked_flag": 0 },
        ...
    ]
    """
    result = []
    try:
        events_ref = (
            db.collection("sessions")
            .document(session_id)
            .collection("song_events")
            .order_by("play_order")
            .stream()
        )
        for event in events_ref:
            data = event.to_dict()
            result.append({
                "video_id":   data.get("video_id",   ""),
                "play_order": data.get("play_order",  0),
                "liked_flag": data.get("liked_flag",  0),
            })
        logger.info(
            f"Fetched play sequence of {len(result)} songs "
            f"for session {session_id}."
        )
    except Exception as e:
        logger.error(
            f"Failed to fetch play sequence for session {session_id}: {e}"
        )
    return result


# ── Session users ─────────────────────────────────────────────────────────────

def get_session_user_ids(db: firestore.Client, session_id: str) -> list:
    """
    Fetches the list of active user_ids in the session.
    Used to know whose liked/disliked songs to fetch from BigQuery.

    Returns:
    ["user_1", "user_2", "user_3"]
    """
    try:
        doc = db.collection("sessions").document(session_id).get()
        if not doc.exists:
            logger.error(f"Session {session_id} not found in Firestore.")
            return []
        data     = doc.to_dict()
        users    = data.get("users", [])
        user_ids = [
            u["user_id"] for u in users
            if u.get("isactive", True) and u.get("user_id")
        ]
        logger.info(
            f"Found {len(user_ids)} active users in session {session_id}."
        )
        return user_ids
    except Exception as e:
        logger.error(
            f"Failed to fetch users for session {session_id}: {e}"
        )
        return []


# ── Session metadata ──────────────────────────────────────────────────────────

def get_songs_played_count(db: firestore.Client, session_id: str) -> int:
    """
    Reads songs_played_count from session metadata.
    Incremented by streaming pipeline after each song plays.
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
            return doc.to_dict().get("songs_played_count", 0)
        return 0
    except Exception as e:
        logger.error(f"Failed to read songs_played_count: {e}")
        return 0


def update_last_refreshed_at(
    db:                 firestore.Client,
    session_id:         str,
    songs_played_count: int,
):
    """
    Updates last_refreshed_at in session metadata.
    Prevents double refresh for the same threshold crossing.
    """
    try:
        (
            db.collection("sessions")
            .document(session_id)
            .collection("metadata")
            .document("state")
            .set({"last_refreshed_at": songs_played_count}, merge=True)
        )
    except Exception as e:
        logger.error(f"Failed to update last_refreshed_at: {e}")


def is_session_active(db: firestore.Client, session_id: str) -> bool:
    """
    Checks if session is still active.
    Returns False if host ended the session.
    """
    try:
        doc = db.collection("sessions").document(session_id).get()
        if not doc.exists:
            return False
        status = doc.to_dict().get("status", "")
        return status in ("active", "done")
    except Exception as e:
        logger.error(f"Failed to check session status: {e}")
        return False