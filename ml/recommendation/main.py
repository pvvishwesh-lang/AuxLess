"""
Main ML recommendation entry point.

Called by ml_trigger.py after publish_session_ready fires.

Flow:
1. initialize_session()  -> load all resources once at session start
2. recommend_next_song() -> called every REFRESH_THRESHOLD songs
3. Results written to Firestore:
   sessions/{session_id}/recommendations/{rank}

Data sources:
  BigQuery  -> song catalog (embeddings) + user liked/disliked songs
  Firestore -> session play sequence, live track scores, session users

Hybrid strategy (three signals):
  CBF: content-based filtering via group preference vector
  CF:  collaborative filtering via population co-occurrence
  GRU: sequential prediction (activates after 3 songs)

GRU activation:
  Songs 1-3:  CBF + CF only (cold start)
  Song 4:     CBF + CF + GRU (gru_weight=0.20, soft entry)
  Song 5:     CBF + CF + GRU (gru_weight=0.25)
  Song 6+:    CBF + CF + GRU (gru_weight=0.30, full weight)

Monitoring:
  After each recommendation cycle, write_rec_snapshot() is called via
  state.monitoring_writer (set by ml_trigger.py after initialize_session).
  state.last_batch_songs is set on every cycle so ml_trigger.py can run
  bias evaluation without re-importing anything from this module.
"""

import json
import logging
import os

import numpy as np
import pandas as pd
from google.cloud import firestore

from ml.recommendation.bigquery_client import (
    get_client,
    fetch_all_embeddings,
    get_user_liked_songs,
    get_user_disliked_songs,
    fetch_all_user_liked,
)
from ml.recommendation.firestore_client import (
    get_db,
    get_session_user_ids,
    get_session_track_scores,
    get_session_play_sequence,
)
from ml.recommendation.user_vectors import (
    build_user_vectors,
    build_global_vector,
    get_already_played_ids,
)
from ml.recommendation.CBF import get_cbf_scores
from ml.recommendation.cf.CF import get_cf_scores
from ml.recommendation.config import (
    PROJECT_ID,
    DATABASE_ID,
    WEIGHT_CBF_COLD,
    WEIGHT_CF_COLD,
    WEIGHT_CBF_WARM,
    WEIGHT_CF_WARM,
    WEIGHT_GRU_WARM,
    GRU_ACTIVATION_THRESHOLD,
    GRU_WEIGHT_RAMP,
    TOP_N,
)
from ml.gru.gru_inference import (
    load_gru_model,
    get_gru_scores,
    build_embedding_lookup,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SESSION_SCORE_WEIGHT = 0.3


# ── Session state ─────────────────────────────────────────────────────────────
class SessionState:
    """
    Holds all resources loaded at session start.
    Avoids repeated BigQuery fetches on every recommendation cycle.

    Monitoring attributes (set by ml_trigger.py after initialize_session):
      catalog_df         — alias for songs_df, used by bias evaluation.
                           popularity_score is not in BigQuery so the bias
                           popularity check falls back to final_score as proxy.
      last_batch_songs   — list[dict] of the last 30 scored recommendations.
                           Set by recommend_next_song() after every cycle.
                           Read by _generate_and_write_batch() in ml_trigger.py
                           to run evaluate_bias() without re-importing main.py.
      monitoring_writer  — MonitoringWriter instance set by ml_trigger.py.
                           Optional: if None, monitoring writes are skipped
                           silently so the pipeline is never blocked.
    """
    def __init__(self):
        self.session_id:       str              = None
        self.db:               firestore.Client = None
        self.bq_client:        object           = None
        self.songs_df:         pd.DataFrame     = None
        self.catalog_df:       pd.DataFrame     = None   # monitoring alias
        self.embedding_lookup: dict             = {}
        self.gru_model:        object           = None
        self.user_ids:         list             = []
        self.all_user_liked:   dict             = {}     # full population for CF
        self.last_batch_songs: list             = []     # monitoring: last batch
        self.monitoring_writer:object           = None   # monitoring: set by trigger


# ── Firestore writer ──────────────────────────────────────────────────────────
def _write_recommendations_to_firestore(
    db:              firestore.Client,
    session_id:      str,
    recommendations: pd.DataFrame,
    gru_active:      bool,
    gru_weight:      float,
):
    """
    Writes top N recommendations to Firestore.
    Frontend watches this collection for real-time updates.
    """
    rec_ref = (
        db.collection("sessions")
        .document(session_id)
        .collection("recommendations")
    )

    for doc in rec_ref.stream():
        doc.reference.delete()

    for rank, (_, row) in enumerate(recommendations.iterrows(), start=1):
        rec_ref.document(str(rank)).set({
            "video_id":       row["video_id"],
            "track_title":    row.get("track_title",  ""),
            "artist_name":    row.get("artist_name",  ""),
            "genre":          row.get("genre",        ""),
            "cbf_score":      round(float(row.get("cbf_score",     0.0)), 4),
            "cf_score":       round(float(row.get("cf_score",      0.0)), 4),
            "session_score":  round(float(row.get("session_score", 0.0)), 4),
            "gru_score":      round(float(row.get("gru_score",     0.0)), 4)
                              if gru_active else None,
            "final_score":    round(float(row["final_score"]), 4),
            "rank":           rank,
            "gru_active":     gru_active,
            "gru_weight":     round(gru_weight, 2),
            "recommended_at": firestore.SERVER_TIMESTAMP,
        })

    logger.info(
        f"Wrote {len(recommendations)} recommendations to Firestore "
        f"for session {session_id} "
        f"(GRU active: {gru_active}, gru_weight: {gru_weight:.2f})"
    )


# ── GRU weight resolver ───────────────────────────────────────────────────────
def _resolve_gru_weight(songs_played: int, gru_active: bool) -> float:
    if not gru_active:
        return 0.0
    return GRU_WEIGHT_RAMP.get(songs_played, WEIGHT_GRU_WARM)


# ── Score aggregation ─────────────────────────────────────────────────────────
def _aggregate_scores(
    cbf_df:       pd.DataFrame,
    cf_df:        pd.DataFrame,
    gru_df:       pd.DataFrame,
    gru_active:   bool,
    songs_played: int,
) -> tuple:
    """
    Combines CBF, CF, session feedback, and GRU scores into a final score.

    Cold start (songs 1-3, GRU not active):
        final = cbf_weight * cbf_score
              + cf_weight * cf_score
              + SESSION_SCORE_WEIGHT * session_score

    Warm (songs 6+):
        final = (cbf_weight/total) * cbf_score
              + (cf_weight/total)  * cf_score
              + (gru_weight/total) * gru_score
              + SESSION_SCORE_WEIGHT * session_score
    """
    if cbf_df.empty:
        logger.warning("CBF returned no results. Cannot aggregate.")
        return pd.DataFrame(), 0.0

    result = cbf_df.copy()

    if "session_score" not in result.columns:
        result["session_score"] = 0.0

    # merge CF scores
    if not cf_df.empty:
        cf_lookup = dict(zip(cf_df["video_id"], cf_df["cf_score"]))
        result["cf_score"] = result["video_id"].map(
            lambda vid: cf_lookup.get(vid, 0.0)
        )
    else:
        result["cf_score"] = 0.0

    gru_weight = _resolve_gru_weight(songs_played, gru_active)

    if not gru_active or gru_df.empty or gru_weight == 0.0:
        # cold start: CBF + CF + session feedback
        total = WEIGHT_CBF_COLD + WEIGHT_CF_COLD
        cbf_w = WEIGHT_CBF_COLD / total
        cf_w  = WEIGHT_CF_COLD  / total

        result["gru_score"]   = 0.0
        result["final_score"] = (
            cbf_w * result["cbf_score"] +
            cf_w  * result["cf_score"]  +
            SESSION_SCORE_WEIGHT * result["session_score"]
        )
        logger.info(
            f"Cold start (songs_played={songs_played}) -- "
            f"CBF: {cbf_w:.2f}, CF: {cf_w:.2f}, GRU: 0.0, "
            f"session_score nudge: {SESSION_SCORE_WEIGHT}"
        )
    else:
        # warm: CBF + CF + GRU with normalized weights
        cbf_weight = WEIGHT_CBF_WARM
        cf_weight  = WEIGHT_CF_WARM
        total      = cbf_weight + cf_weight + gru_weight

        gru_lookup          = dict(zip(gru_df["video_id"], gru_df["gru_score"]))
        result["gru_score"] = result["video_id"].map(
            lambda vid: gru_lookup.get(vid, 0.0)
        )
        result["final_score"] = (
            (cbf_weight / total) * result["cbf_score"]  +
            (cf_weight  / total) * result["cf_score"]   +
            (gru_weight / total) * result["gru_score"]  +
            SESSION_SCORE_WEIGHT * result["session_score"]
        )
        logger.info(
            f"Warm (songs_played={songs_played}) -- "
            f"CBF: {cbf_weight/total:.2f}, "
            f"CF: {cf_weight/total:.2f}, "
            f"GRU: {gru_weight/total:.2f} "
            f"(raw gru_weight={gru_weight}), "
            f"session_score nudge: {SESSION_SCORE_WEIGHT}"
        )

    recommendations = (
        result
        .sort_values("final_score", ascending=False)
        .head(TOP_N)
        .reset_index(drop=True)
    )

    return recommendations, gru_weight


# ── Monitoring helpers ────────────────────────────────────────────────────────
def _resolve_phase_name(songs_played: int, gru_active: bool) -> str:
    """
    Returns the phase string that matches what the dashboard displays.
    Mirrors the weight ramp logic in _aggregate_scores.
    """
    if not gru_active:
        return "cold_start"
    gru_weight = GRU_WEIGHT_RAMP.get(songs_played, WEIGHT_GRU_WARM)
    if gru_weight == 0.20:
        return "gru_soft_entry"
    if gru_weight == 0.25:
        return "gru_growing"
    return "full_hybrid"


def _resolve_active_weights(
    songs_played: int,
    gru_active:   bool,
    gru_weight:   float,
) -> dict:
    """
    Returns the normalised weights dict used in this cycle.
    Matches the exact values computed in _aggregate_scores.
    """
    if not gru_active or gru_weight == 0.0:
        total = WEIGHT_CBF_COLD + WEIGHT_CF_COLD
        return {
            "cbf": round(WEIGHT_CBF_COLD / total, 4),
            "cf":  round(WEIGHT_CF_COLD  / total, 4),
            "gru": 0.0,
        }
    total = WEIGHT_CBF_WARM + WEIGHT_CF_WARM + gru_weight
    return {
        "cbf": round(WEIGHT_CBF_WARM / total, 4),
        "cf":  round(WEIGHT_CF_WARM  / total, 4),
        "gru": round(gru_weight      / total, 4),
    }


def _build_scored_songs(recommendations: pd.DataFrame) -> list[dict]:
    """
    Converts the recommendations DataFrame into a list of dicts for monitoring.

    Column mapping:
      track_title  → title    (dashboard field name)
      artist_name  → artist   (dashboard field name)

    popularity_score is intentionally omitted — it is not stored in BigQuery
    (it lives in the GCS preprocessed CSV). evaluate() in model_bias.py
    handles the missing column gracefully by using final_score as a proxy.
    """
    scored = []
    for _, row in recommendations.iterrows():
        scored.append({
            "video_id":        str(row.get("video_id",     "")),
            "title":           str(row.get("track_title",  "")),
            "artist":          str(row.get("artist_name",  "")),
            "genre":           str(row.get("genre",        "")),
            "cbf_score":       round(float(row.get("cbf_score",  0.0)), 4),
            "cf_score":        round(float(row.get("cf_score",   0.0)), 4),
            "gru_score":       round(float(row.get("gru_score",  0.0)), 4),
            "final_score":     round(float(row.get("final_score",0.0)), 4),
        })
    return scored


# ── Session initialization ────────────────────────────────────────────────────
def initialize_session(session_id: str) -> SessionState:
    """
    Called once when session_ready fires.
    Loads all heavy resources into memory.
    """
    logger.info(f"Initializing ML session: {session_id}")
    state            = SessionState()
    state.session_id = session_id

    state.db        = get_db(PROJECT_ID, DATABASE_ID)
    state.bq_client = get_client()

    logger.info("Fetching song catalog from BigQuery...")
    state.songs_df         = fetch_all_embeddings(state.bq_client)
    state.catalog_df       = state.songs_df          # monitoring alias
    state.embedding_lookup = build_embedding_lookup(state.songs_df)
    logger.info(f"Loaded {len(state.songs_df)} songs from BigQuery")

    try:
        state.gru_model = load_gru_model()
    except Exception as e:
        logger.warning(
            f"GRU model could not be loaded: {e}. Using CBF + CF only."
        )
        state.gru_model = None
        try:
            from google.cloud import pubsub_v1
            alert_topic = os.environ.get("ALERT_TOPIC", "auxless-alerts")
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(PROJECT_ID, alert_topic)
            alert_payload = json.dumps({
                "session_id": session_id,
                "error": str(e),
                "message": f"GRU model failed to load for session {session_id}. Falling back to CBF+CF only.",
                "severity": "WARNING"
            }).encode("utf-8")
            publisher.publish(topic_path, alert_payload)
            logger.info(f"GRU fallback alert published for session {session_id}")
        except Exception as alert_err:
            logger.error(f"Failed to publish GRU fallback alert: {alert_err}")

    state.user_ids = get_session_user_ids(state.db, session_id)
    logger.info(f"Session users: {state.user_ids}")

    # fetch full population liked songs for CF co-occurrence
    logger.info("Fetching population liked songs for CF...")
    state.all_user_liked = fetch_all_user_liked(state.bq_client)
    logger.info(f"CF population: {len(state.all_user_liked)} users")

    logger.info(f"Session {session_id} initialized.")
    return state


# ── Recommendation cycle ──────────────────────────────────────────────────────
def recommend_next_song(state: SessionState) -> dict:
    """
    Called every REFRESH_THRESHOLD songs by ml_trigger.py.

    Reads latest session state from Firestore + BigQuery,
    runs CBF + CF + GRU (if active), aggregates with graduated
    GRU weight ramp, writes top N to Firestore.

    Monitoring side effects (non-critical, never raise):
      - Sets state.last_batch_songs  → read by ml_trigger.py for bias eval
      - Calls state.monitoring_writer.write_rec_snapshot() if writer exists
    """
    session_id = state.session_id
    db         = state.db
    logger.info(f"Running recommendation cycle for session {session_id}")

    # read session state from Firestore
    play_sequence  = get_session_play_sequence(db, session_id)
    track_scores   = get_session_track_scores(db, session_id)
    already_played = get_already_played_ids(play_sequence)

    # read user liked/disliked songs from BigQuery
    user_liked    = get_user_liked_songs(state.bq_client, state.user_ids)
    user_disliked = get_user_disliked_songs(state.bq_client, state.user_ids)

    songs_played = len(play_sequence)
    gru_active   = (
        state.gru_model is not None and
        songs_played >= GRU_ACTIVATION_THRESHOLD
    )

    logger.info(
        f"Songs played: {songs_played} | "
        f"GRU active: {gru_active} | "
        f"Already played: {len(already_played)}"
    )

    # build user vectors
    if user_liked:
        user_vectors  = build_user_vectors(
            user_liked, user_disliked, state.songs_df
        )
        global_vector = build_global_vector(user_vectors)
    else:
        logger.warning(
            "No user liked songs found in BigQuery. "
            "Using average catalog embedding as fallback."
        )
        all_embeddings = np.stack(state.songs_df["embedding"].values)
        global_vector  = np.mean(all_embeddings, axis=0, keepdims=True)

    # CBF
    cbf_df = get_cbf_scores(
        global_vector=global_vector,
        songs_df=state.songs_df,
        user_liked_songs=user_liked,
        user_disliked_songs=user_disliked,
        already_played_ids=already_played,
        track_scores=track_scores,
        top_n=TOP_N,
    )

    # CF — returns scores for ALL candidates (not top_n).
    # _aggregate_scores() looks up cf_score per CBF candidate via dict,
    # so returning the full scored set avoids zeros from set mismatch.
    cf_df = get_cf_scores(
        all_user_liked=state.all_user_liked,
        user_liked_songs=user_liked,
        songs_df=state.songs_df,
        already_played_ids=already_played,
        state=state,
    )

    # GRU
    gru_df = pd.DataFrame()
    if gru_active:
        gru_df = get_gru_scores(
            model=state.gru_model,
            play_sequence=play_sequence,
            embedding_lookup=state.embedding_lookup,
            songs_df=state.songs_df,
            already_played_ids=already_played,
            top_n=TOP_N,
        )

    # aggregate
    recommendations, gru_weight = _aggregate_scores(
        cbf_df=cbf_df,
        cf_df=cf_df,
        gru_df=gru_df,
        gru_active=gru_active,
        songs_played=songs_played,
    )

    if recommendations.empty:
        logger.error("No recommendations generated.")
        return {}

    # ── Monitoring ────────────────────────────────────────────────────────────
    # Runs after aggregation, before Firestore write.
    # Failures here are non-critical and must never block the pipeline.
    try:
        scored_songs = _build_scored_songs(recommendations)

        # always update last_batch_songs so ml_trigger.py can run bias eval
        state.last_batch_songs = scored_songs

        # write rec snapshot if monitoring_writer is wired up
        if state.monitoring_writer is not None:
            phase   = _resolve_phase_name(songs_played, gru_active)
            weights = _resolve_active_weights(songs_played, gru_active, gru_weight)
            state.monitoring_writer.write_rec_snapshot(
                phase=phase,
                weights=weights,
                scored_songs=scored_songs,
            )
            logger.debug(
                "Rec snapshot written — phase: %s, weights: %s, songs: %d",
                phase, weights, len(scored_songs),
            )
    except Exception as exc:
        logger.error("Monitoring write failed (non-critical): %s", exc)
    # ── End monitoring ────────────────────────────────────────────────────────

    # write to Firestore for frontend
    _write_recommendations_to_firestore(
        db=db,
        session_id=session_id,
        recommendations=recommendations,
        gru_active=gru_active,
        gru_weight=gru_weight,
    )

    top_song = recommendations.iloc[0].to_dict()
    logger.info(
        f"Top recommendation: '{top_song['track_title']}' "
        f"by {top_song['artist_name']} "
        f"(final_score={top_song['final_score']:.4f}, "
        f"gru_weight={gru_weight:.2f})"
    )
    return top_song


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m ml.recommendation.main <session_id>")
        sys.exit(1)

    session_id = sys.argv[1]
    state      = initialize_session(session_id)
    top_song   = recommend_next_song(state)
    print(f"\nTop recommendation: {top_song}")
