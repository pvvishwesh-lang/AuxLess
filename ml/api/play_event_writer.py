"""
play_event_writer.py
--------------------
Called from the /feedback endpoint in server.py after publish_feedback_event
succeeds. Writes a play_event document to Firestore for the monitoring
dashboard's System Stats and Session State panels.

Why here and not in the Dataflow streaming consumer:
  The streaming consumer runs in a separate Dataflow pipeline with its own
  lifecycle. Putting monitoring writes there would require wiring a separate
  Firestore client into that pipeline and coupling two independent systems.
  The /feedback endpoint already has everything needed: session_id, video_id,
  action, and access to Firestore. One extra non-blocking write here is the
  right tradeoff.

Song metadata (genre, title, artist) is resolved by looking up the video_id
in sessions/{session_id}/recommendations/ — which main.py writes on every
recommendation cycle. If the song isn't found there (e.g. the recommendation
collection was cleared), the write still succeeds with "unknown" metadata
so the feedback type and timing are always captured.
"""

import logging
import os
import functools
import time

logger = logging.getLogger(__name__)

# maps /feedback action field to MonitoringWriter feedback values
_ACTION_MAP = {
    "like":    "like",
    "dislike": "dislike",
    "skip":    "skip",
    "replay":  "replay",
}


@functools.lru_cache(maxsize=1)
def _get_db():
    """
    Returns a cached Firestore client.
    Uses the same env vars as the rest of server.py.
    """
    from google.cloud import firestore
    project = os.environ.get("PROJECT_ID", "")
    db_name = os.environ.get("FIRESTORE_DATABASE", "auxless")
    return firestore.Client(project=project, database=db_name)


def _lookup_song_metadata(db, session_id: str, video_id: str) -> dict:
    """
    Looks up title, artist, genre for a video_id from the recommendations
    collection that main.py writes on every cycle.

    Returns a dict with title, artist, genre — falls back to empty strings
    if the song is not found so the play_event write always succeeds.
    """
    try:
        recs_ref = (
            db.collection("sessions")
            .document(session_id)
            .collection("recommendations")
            .stream()
        )
        for doc in recs_ref:
            data = doc.to_dict()
            if data.get("video_id") == video_id:
                return {
                    "title":  data.get("track_title", ""),
                    "artist": data.get("artist_name", ""),
                    "genre":  data.get("genre", ""),
                }
    except Exception as exc:
        logger.warning(
            "Could not resolve song metadata for %s in session %s: %s",
            video_id, session_id, exc,
        )
    return {"title": "", "artist": "", "genre": ""}


def _get_songs_played_count(db, session_id: str) -> int:
    """
    Reads songs_played_count from the session document.
    Used to record how many songs had played before this feedback event.
    Returns 0 if unavailable so the write always succeeds.
    """
    try:
        doc = db.collection("sessions").document(session_id).get()
        if doc.exists:
            return int(doc.to_dict().get("songs_played_count", 0))
    except Exception as exc:
        logger.warning(
            "Could not read songs_played_count for session %s: %s",
            session_id, exc,
        )
    return 0


def write_play_event_from_feedback(
    session_id:       str,
    video_id:         str,
    action:           str,
    play_duration_sec: float = 0.0,
) -> None:
    """
    Called from the /feedback endpoint in server.py after
    publish_feedback_event() succeeds.

    Resolves song metadata from Firestore, then writes a play_event
    document to sessions/{session_id}/play_events/{ts_ms}.

    Args:
        session_id:        from the feedback request body
        video_id:          from the feedback request body
        action:            one of like | dislike | skip | replay
        play_duration_sec: seconds the song played before this action.
                           Optional — frontend should send it when available.
                           Defaults to 0.0 if not provided.

    This function never raises. Any failure is logged and swallowed so
    a monitoring write can never break the feedback pipeline.
    """
    try:
        db           = _get_db()
        feedback     = _ACTION_MAP.get(action, "neutral")
        metadata     = _lookup_song_metadata(db, session_id, video_id)
        songs_before = _get_songs_played_count(db, session_id)

        from ml.recommendation.firestore_monitoring import MonitoringWriter
        writer = MonitoringWriter(db, session_id)
        writer.write_play_event(
            song={
                "video_id": video_id,
                "title":    metadata["title"],
                "artist":   metadata["artist"],
                "genre":    metadata["genre"],
            },
            feedback=feedback,
            play_duration_sec=float(play_duration_sec),
            songs_played_before=songs_before,
        )
        logger.debug(
            "play_event written — session: %s, video_id: %s, action: %s, duration: %.1fs",
            session_id, video_id, action, play_duration_sec,
        )
    except Exception as exc:
        # monitoring must never crash the feedback pipeline
        logger.error(
            "write_play_event_from_feedback failed (non-critical): %s", exc
        )
