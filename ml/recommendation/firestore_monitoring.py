"""
firestore_monitoring.py
-----------------------
Monitoring write/read helpers that extend your existing firestore_client.py.
Import these alongside your existing FirestoreClient and call them from
ml_trigger.py and recommendation/main.py after each recommendation cycle.

Firestore schema written by this module:

  sessions/{session_id}/
    rec_snapshots/{timestamp_ms}/         <- one doc per regen cycle
      phase: str
      weights: {cbf, cf, gru}
      songs: [
        {
          video_id, title, artist, genre,
          cbf_score, cf_score, gru_score, final_score,
          rank
        }, ...
      ]
      generated_at: timestamp

    bias_snapshots/{timestamp_ms}/        <- one doc per regen cycle
      genre_distribution: {genre: pct}
      genre_flags: [str]
      popularity_ratio: float
      popularity_flagged: bool
      score_disparity: float
      disparity_flagged: bool
      diversity_score: float              <- Shannon entropy
      generated_at: timestamp

    play_events/{timestamp_ms}/           <- one doc per song played
      video_id, title, artist, genre
      feedback: like | skip | replay | dislike | neutral
      play_duration_sec: float
      played_at: timestamp
      songs_played_before: int            <- count before this song
"""

from __future__ import annotations

import time
import math
import logging
from typing import Any

from google.cloud import firestore

logger = logging.getLogger(__name__)

_SESSIONS = "sessions"
_REC_SNAP = "rec_snapshots"
_BIAS_SNAP = "bias_snapshots"
_PLAY_EVENTS = "play_events"


class MonitoringWriter:
    """
    Thin wrapper around a Firestore client that writes monitoring documents.
    Initialise once per session from ml_trigger.py, reuse across cycles.

    Usage:
        writer = MonitoringWriter(db, session_id)
        writer.write_rec_snapshot(phase, weights, scored_songs)
        writer.write_bias_snapshot(bias_result)
        writer.write_play_event(song, feedback, duration_sec, songs_before)
    """

    def __init__(self, db: firestore.Client, session_id: str) -> None:
        self.db = db
        self.session_id = session_id
        self._session_ref = db.collection(_SESSIONS).document(session_id)

    # ------------------------------------------------------------------
    # Recommendation snapshot
    # ------------------------------------------------------------------

    def write_rec_snapshot(
        self,
        phase: str,
        weights: dict[str, float],
        scored_songs: list[dict[str, Any]],
    ) -> None:
        """
        Call this immediately after main.py generates a recommendation batch.

        scored_songs must be a list of dicts, each containing at minimum:
            video_id, title, artist, genre,
            cbf_score, cf_score, gru_score, final_score
        They should already be in ranked order (rank 1 = index 0).
        """
        ts_ms = _ts_ms()
        doc = {
            "phase": phase,
            "weights": {
                "cbf": round(weights.get("cbf", 0.0), 4),
                "cf": round(weights.get("cf", 0.0), 4),
                "gru": round(weights.get("gru", 0.0), 4),
            },
            "songs": [
                {
                    "rank": idx + 1,
                    "video_id": s.get("video_id", ""),
                    "title": s.get("title", ""),
                    "artist": s.get("artist", ""),
                    "genre": s.get("genre", ""),
                    "cbf_score": round(float(s.get("cbf_score", 0.0)), 4),
                    "cf_score": round(float(s.get("cf_score", 0.0)), 4),
                    "gru_score": round(float(s.get("gru_score", 0.0)), 4),
                    "final_score": round(float(s.get("final_score", 0.0)), 4),
                }
                for idx, s in enumerate(scored_songs[:30])  # cap at 30
            ],
            "generated_at": firestore.SERVER_TIMESTAMP,
            "ts_ms": ts_ms,
        }
        try:
            self._session_ref.collection(_REC_SNAP).document(str(ts_ms)).set(doc)
            logger.info("rec_snapshot written for session %s", self.session_id)
        except Exception as exc:
            logger.error("Failed to write rec_snapshot: %s", exc)

    # ------------------------------------------------------------------
    # Bias snapshot
    # ------------------------------------------------------------------

    def write_bias_snapshot(self, bias_result: dict[str, Any]) -> None:
        """
        bias_result is the dict returned by model_bias.generate_bias_report()
        via the evaluate() adapter.

        Real output shape from generate_bias_report():
            {
              "genre_representation": {
                  "recommendation_proportions": {genre: float},
                  "catalog_proportions":        {genre: float},
                  "deviations":                 {genre: float},
                  "over_represented":           [str],
                  "under_represented":          [str],
                  "recommendation_entropy":     float,
                  "catalog_entropy":            float,
                  "entropy_ratio":              float,
                  "genre_bias_detected":        bool,
              },
              "popularity_bias": {
                  "rec_mean_popularity":        float,
                  "catalog_mean_popularity":    float,
                  "rec_median_popularity":      float,
                  "catalog_median_popularity":  float,
                  "popularity_bias_ratio":      float,
                  "popularity_bias_detected":   bool,
              },
              "score_disparity": {
                  "per_genre_scores":           {genre: {mean, std, count}},
                  "max_score_disparity":        float,
                  "disparity_threshold":        float,
                  "score_disparity_detected":   bool,
              },
              "overall_bias_detected":          bool,
              "recommendation_count":           int,
              "catalog_count":                  int,
              "mitigation_suggestions":         [str],
            }
        """
        ts_ms = _ts_ms()

        # ── unpack nested sub-dicts ────────────────────────────────────────
        genre_rep   = bias_result.get("genre_representation", {})
        pop_bias    = bias_result.get("popularity_bias", {})
        score_disp  = bias_result.get("score_disparity", {})

        # genre distribution: use recommendation proportions for display
        genre_dist: dict[str, float] = genre_rep.get("recommendation_proportions", {})

        # build genre flags list from over/under represented
        genre_flags: list[str] = []
        for g in genre_rep.get("over_represented", []):
            genre_flags.append(f"{g}_over_represented")
        for g in genre_rep.get("under_represented", []):
            genre_flags.append(f"{g}_under_represented")

        # per-genre score means — flatten for easy dashboard consumption
        per_genre_scores: dict[str, float] = {
            genre: round(vals.get("mean_score", 0.0), 4)
            for genre, vals in score_disp.get("per_genre_scores", {}).items()
        }

        doc = {
            # ── genre representation ───────────────────────────────────────
            "genre_distribution":        genre_dist,
            "catalog_distribution":      genre_rep.get("catalog_proportions", {}),
            "genre_deviations":          genre_rep.get("deviations", {}),
            "genre_flags":               genre_flags,
            "over_represented":          genre_rep.get("over_represented", []),
            "under_represented":         genre_rep.get("under_represented", []),
            "genre_bias_detected":       genre_rep.get("genre_bias_detected", False),

            # ── diversity ─────────────────────────────────────────────────
            "diversity_score":           round(float(genre_rep.get("recommendation_entropy", 0.0)), 4),
            "catalog_entropy":           round(float(genre_rep.get("catalog_entropy", 0.0)), 4),
            "entropy_ratio":             round(float(genre_rep.get("entropy_ratio", 1.0)), 4),

            # ── popularity bias ───────────────────────────────────────────
            "popularity_ratio":          round(float(pop_bias.get("popularity_bias_ratio", 1.0)), 4),
            "rec_mean_popularity":       round(float(pop_bias.get("rec_mean_popularity", 0.0)), 4),
            "catalog_mean_popularity":   round(float(pop_bias.get("catalog_mean_popularity", 0.0)), 4),
            "popularity_flagged":        pop_bias.get("popularity_bias_detected", False),

            # ── score disparity ───────────────────────────────────────────
            "score_disparity":           round(float(score_disp.get("max_score_disparity", 0.0)), 4),
            "disparity_threshold":       round(float(score_disp.get("disparity_threshold", 0.15)), 4),
            "disparity_flagged":         score_disp.get("score_disparity_detected", False),
            "per_genre_scores":          per_genre_scores,

            # ── overall ───────────────────────────────────────────────────
            "overall_bias_detected":     bias_result.get("overall_bias_detected", False),
            "recommendation_count":      bias_result.get("recommendation_count", 0),
            "mitigation_suggestions":    bias_result.get("mitigation_suggestions", []),

            "generated_at": firestore.SERVER_TIMESTAMP,
            "ts_ms": ts_ms,
        }
        try:
            self._session_ref.collection(_BIAS_SNAP).document(str(ts_ms)).set(doc)
            logger.info("bias_snapshot written for session %s", self.session_id)
        except Exception as exc:
            logger.error("Failed to write bias_snapshot: %s", exc)

    # ------------------------------------------------------------------
    # Play event (called by your streaming/feedback pipeline)
    # ------------------------------------------------------------------

    def write_play_event(
        self,
        song: dict[str, Any],
        feedback: str,
        play_duration_sec: float,
        songs_played_before: int,
    ) -> None:
        """
        feedback: one of 'like' | 'skip' | 'replay' | 'dislike' | 'neutral'
        play_duration_sec: actual seconds the song played before feedback/next
        songs_played_before: how many songs had played before this one (0-indexed)
        """
        ts_ms = _ts_ms()
        doc = {
            "video_id": song.get("video_id", ""),
            "title": song.get("title", ""),
            "artist": song.get("artist", ""),
            "genre": song.get("genre", ""),
            "feedback": feedback,
            "play_duration_sec": round(float(play_duration_sec), 2),
            "songs_played_before": songs_played_before,
            "played_at": firestore.SERVER_TIMESTAMP,
            "ts_ms": ts_ms,
        }
        try:
            self._session_ref.collection(_PLAY_EVENTS).document(str(ts_ms)).set(doc)
        except Exception as exc:
            logger.error("Failed to write play_event: %s", exc)


# ------------------------------------------------------------------
# Read helpers (called by monitoring_routes.py)
# ------------------------------------------------------------------

def read_session_state(db: firestore.Client, session_id: str) -> dict[str, Any]:
    """
    Returns the current session state for the dashboard Session State panel.

    Data sources (from Firestore subcollections written by the frontend):
      songs_played    → song_events (one doc per song played; has genre, play_order)
      feedback_counts → feedback_events (event_type: like/dislike/skip/replay)
      user_count      → users array on root session document
      phase/weights   → latest rec_snapshot (written by ML pipeline)
    """
    session_ref = db.collection(_SESSIONS).document(session_id)
    session_doc = session_ref.get()
    if not session_doc.exists:
        return {"error": f"Session {session_id} not found"}

    data = session_doc.to_dict() or {}

    # ── user_count ────────────────────────────────────────────────────────
    users = data.get("users", [])
    user_count = len(users) if users else data.get("user_count", 0)

    # ── songs_played from song_events ─────────────────────────────────────
    # Schema: { artist, genre, liked_flag, play_order, session_number,
    #           song_name, timestamp, video_id }
    songs_played = 0
    try:
        song_docs = list(
            session_ref.collection("song_events").limit(500).stream()
        )
        songs_played = len(song_docs)
    except Exception as exc:
        logger.debug("Could not read song_events: %s", exc)

    # fallback: metadata or session doc
    if songs_played == 0:
        songs_played = data.get("songs_played_count", 0)
        if songs_played == 0:
            try:
                meta_doc = session_ref.collection("metadata").document("state").get()
                if meta_doc.exists:
                    md = meta_doc.to_dict()
                    songs_played = md.get("songs_played_count",
                                   md.get("last_refreshed_at", 0))
            except Exception:
                pass

    # ── feedback_counts from feedback_events ──────────────────────────────
    # Schema: { event_type, artist, song_name, song_id, room_id,
    #           user_id, timestamp }
    feedback_counts = {"like": 0, "skip": 0, "replay": 0, "dislike": 0, "neutral": 0}
    try:
        fb_docs = list(
            session_ref.collection("feedback_events").limit(500).stream()
        )
        for evt in fb_docs:
            e = evt.to_dict()
            fb = e.get("event_type",
                 e.get("action",
                 e.get("feedback", "neutral")))
            if fb in ("like", "dislike", "skip", "replay"):
                feedback_counts[fb] = feedback_counts.get(fb, 0) + 1
            else:
                feedback_counts["neutral"] += 1
    except Exception as exc:
        logger.debug("Could not read feedback_events: %s", exc)

    latest_rec = _latest_doc(session_ref, _REC_SNAP)

    return {
        "session_id": session_id,
        "phase": latest_rec.get("phase", "cold_start") if latest_rec else "cold_start",
        "weights": latest_rec.get("weights", {"cbf": 0.6, "cf": 0.4, "gru": 0.0}) if latest_rec else {"cbf": 0.6, "cf": 0.4, "gru": 0.0},
        "songs_played": songs_played,
        "feedback_counts": feedback_counts,
        "avg_play_duration_sec": 0.0,
        "status": data.get("status", "active"),
        "user_count": user_count,
        "created_at": _serialize_ts(data.get("created_at")),
    }


def read_recommendations(
    db: firestore.Client, session_id: str, limit: int = 5
) -> list[dict[str, Any]]:
    """
    Returns the last `limit` recommendation cycles with full score breakdowns.
    Used by the Recommendation Inspector panel.
    """
    session_ref = db.collection(_SESSIONS).document(session_id)
    docs = (
        session_ref.collection(_REC_SNAP)
        .order_by("ts_ms", direction=firestore.Query.DESCENDING)
        .limit(limit)
        .stream()
    )
    results = []
    for doc in docs:
        d = doc.to_dict()
        results.append({
            "ts_ms": d.get("ts_ms"),
            "phase": d.get("phase"),
            "weights": d.get("weights"),
            "songs": d.get("songs", []),
        })
    return results


def read_bias(db: firestore.Client, session_id: str) -> dict[str, Any]:
    """Latest bias snapshot for the Bias Monitor panel."""
    session_ref = db.collection(_SESSIONS).document(session_id)
    latest = _latest_doc(session_ref, _BIAS_SNAP)
    return latest or {
        "genre_distribution": {}, "catalog_distribution": {},
        "genre_deviations": {}, "genre_flags": [],
        "over_represented": [], "under_represented": [],
        "genre_bias_detected": False,
        "diversity_score": 0.0, "catalog_entropy": 0.0, "entropy_ratio": 1.0,
        "popularity_ratio": 1.0, "rec_mean_popularity": 0.0,
        "catalog_mean_popularity": 0.0, "popularity_flagged": False,
        "score_disparity": 0.0, "disparity_threshold": 0.15,
        "disparity_flagged": False, "per_genre_scores": {},
        "overall_bias_detected": False, "recommendation_count": 0,
        "mitigation_suggestions": [],
    }


def read_system_stats(
    db: firestore.Client, session_id: str
) -> dict[str, Any]:
    """
    Aggregates song_events and feedback_events for the System Stats panel:
    genre distribution, popularity histogram buckets,
    diversity trend (entropy per rec cycle), avg feedback score.
    """
    session_ref = db.collection(_SESSIONS).document(session_id)

    # ── genre distribution from song_events ───────────────────────────────
    # Schema: { artist, genre, liked_flag, play_order, session_number,
    #           song_name, timestamp, video_id }
    genre_counts: dict[str, int] = {}
    total_plays = 0
    try:
        song_docs = list(
            session_ref.collection("song_events").limit(500).stream()
        )
        total_plays = len(song_docs)
        for doc in song_docs:
            e = doc.to_dict()
            genre = e.get("genre", "Unknown")
            genre_counts[genre] = genre_counts.get(genre, 0) + 1
    except Exception:
        pass

    genre_distribution = {
        g: round(c / total_plays, 4) if total_plays else 0
        for g, c in sorted(genre_counts.items(), key=lambda x: -x[1])
    }

    # ── feedback scores from feedback_events ──────────────────────────────
    # Schema: { event_type, artist, song_name, song_id, room_id,
    #           user_id, timestamp }
    fb_score_map = {"like": 1.0, "replay": 0.75, "neutral": 0.0, "skip": -0.5, "dislike": -1.0}
    feedback_scores: list[float] = []
    try:
        fb_docs = list(
            session_ref.collection("feedback_events").limit(500).stream()
        )
        for doc in fb_docs:
            e = doc.to_dict()
            fb = e.get("event_type",
                 e.get("action",
                 e.get("feedback", "neutral")))
            if fb in ("play", "complete"):
                fb = "neutral"
            feedback_scores.append(fb_score_map.get(fb, 0.0))
    except Exception:
        pass

    # popularity histogram: bucket final_scores from rec snapshots
    rec_docs = list(
        session_ref.collection(_REC_SNAP)
        .order_by("ts_ms")
        .limit(20)
        .stream()
    )
    all_scores: list[float] = []
    diversity_trend: list[dict] = []
    for rdoc in rec_docs:
        rd = rdoc.to_dict()
        songs = rd.get("songs", [])
        for s in songs:
            all_scores.append(s.get("final_score", 0.0))
        # genre entropy per cycle
        cycle_genres: dict[str, int] = {}
        for s in songs:
            g = s.get("genre", "Unknown")
            cycle_genres[g] = cycle_genres.get(g, 0) + 1
        diversity_trend.append({
            "ts_ms": rd.get("ts_ms"),
            "entropy": round(_shannon_entropy({g: c / len(songs) for g, c in cycle_genres.items()} if songs else {}), 4),
            "song_count": len(songs),
        })

    pop_histogram = _histogram(all_scores, bins=10, lo=0.0, hi=1.0)

    avg_feedback_score = round(sum(feedback_scores) / len(feedback_scores), 4) if feedback_scores else 0.0

    return {
        "total_plays": total_plays,
        "genre_distribution": genre_distribution,
        "popularity_histogram": pop_histogram,
        "diversity_trend": diversity_trend,
        "avg_play_duration_sec": 0.0,
        "avg_feedback_score": avg_feedback_score,
        "feedback_score_interpretation": _feedback_interpretation(avg_feedback_score),
    }


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------

def _ts_ms() -> int:
    return int(time.time() * 1000)


def _shannon_entropy(distribution: dict[str, float]) -> float:
    entropy = 0.0
    for p in distribution.values():
        if p > 0:
            entropy -= p * math.log2(p)
    return entropy


def _latest_doc(session_ref, subcollection: str) -> dict[str, Any] | None:
    docs = list(
        session_ref.collection(subcollection)
        .order_by("ts_ms", direction=firestore.Query.DESCENDING)
        .limit(1)
        .stream()
    )
    return docs[0].to_dict() if docs else None


def _histogram(
    values: list[float], bins: int = 10, lo: float = 0.0, hi: float = 1.0
) -> list[dict]:
    step = (hi - lo) / bins
    buckets = [{"lo": round(lo + i * step, 2), "hi": round(lo + (i + 1) * step, 2), "count": 0} for i in range(bins)]
    for v in values:
        idx = min(int((v - lo) / step), bins - 1)
        if 0 <= idx < bins:
            buckets[idx]["count"] += 1
    return buckets


def _serialize_ts(ts) -> str | None:
    if ts is None:
        return None
    try:
        return ts.isoformat()
    except Exception:
        return str(ts)


def _feedback_interpretation(score: float) -> str:
    if score >= 0.5:
        return "Excellent — group is highly engaged"
    if score >= 0.2:
        return "Good — recommendations landing well"
    if score >= -0.1:
        return "Neutral — room for improvement"
    if score >= -0.4:
        return "Below average — consider bias checks"
    return "Poor — review recommendation weights"