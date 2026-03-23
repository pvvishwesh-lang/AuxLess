"""
Main ML recommendation entry point.

Called by ml_trigger.py after publish_session_ready fires.

Flow:
1. initialize_session()  → load all resources once at session start
2. recommend_next_song() → called every REFRESH_THRESHOLD songs
3. Results written to Firestore:
   sessions/{session_id}/recommendations/{rank}

Data sources:
  BigQuery  → song catalog (embeddings) + user liked/disliked songs
  Firestore → session play sequence, live track scores, session users

GRU activation strategy:
  Songs 1-3:  CBF + CF only (cold start)
  Song 4:     CBF + CF + GRU (gru_weight=0.20, soft entry)
  Song 5:     CBF + CF + GRU (gru_weight=0.25)
  Song 6+:    CBF + CF + GRU (gru_weight=0.30, full weight)
"""

import logging

import numpy as np
import pandas as pd
from google.cloud import firestore

from ml.recommendation.bigquery_client import (
    get_client,
    fetch_all_embeddings,
    get_user_liked_songs,
    get_user_disliked_songs,
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

# how strongly session feedback nudges final scores (must match CBF.py)
SESSION_SCORE_WEIGHT = 0.3


# ── Session state ─────────────────────────────────────────────────────────────
class SessionState:
    """
    Holds all resources loaded at session start.
    Avoids repeated BigQuery fetches on every recommendation cycle.
    """
    def __init__(self):
        self.session_id:       str              = None
        self.db:               firestore.Client = None
        self.bq_client:        object           = None
        self.songs_df:         pd.DataFrame     = None
        self.embedding_lookup: dict             = {}
        self.gru_model:        object           = None
        self.user_ids:         list             = []


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

    Path: sessions/{session_id}/recommendations/{rank}
    """
    rec_ref = (
        db.collection("sessions")
        .document(session_id)
        .collection("recommendations")
    )

    # clear previous recommendations
    for doc in rec_ref.stream():
        doc.reference.delete()

    # write new recommendations
    for rank, (_, row) in enumerate(recommendations.iterrows(), start=1):
        rec_ref.document(str(rank)).set({
            "video_id":       row["video_id"],
            "track_title":    row.get("track_title",  ""),
            "artist_name":    row.get("artist_name",  ""),
            "genre":          row.get("genre",        ""),
            "cbf_score":      round(float(row.get("cbf_score",     0.0)), 4),
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
    """
    Resolves the GRU weight based on how many songs have played.

    Songs 1-3:  GRU not active → weight = 0.0
    Song 4:     GRU enters softly → weight = 0.20
    Song 5:     GRU grows → weight = 0.25
    Song 6+:    GRU at full weight → weight = 0.30
    """
    if not gru_active:
        return 0.0
    return GRU_WEIGHT_RAMP.get(songs_played, WEIGHT_GRU_WARM)


# ── Score aggregation ─────────────────────────────────────────────────────────
def _aggregate_scores(
    cbf_df:       pd.DataFrame,
    gru_df:       pd.DataFrame,
    gru_active:   bool,
    songs_played: int,
) -> tuple:
    """
    Combines CBF, session feedback, and GRU scores into a final score.

    Cold start (songs 1-3, GRU not active):
        final = cbf_weight * cbf_score
              + (1 - cbf_weight) * cf_score          (cf placeholder = 0)
              + SESSION_SCORE_WEIGHT * session_score  (live feedback nudge)

    Gradual ramp (songs 4-5) and full warm (songs 6+):
        final = (cbf_weight/total) * cbf_score
              + (cf_weight/total)  * cf_score         (cf placeholder = 0)
              + (gru_weight/total) * gru_score
              + SESSION_SCORE_WEIGHT * session_score  (live feedback nudge)

    session_score comes from CBF.py — normalized Firestore track scores.
    It is kept additive (not part of the normalized weight sum) so live
    feedback nudges scores without disrupting the CBF/CF/GRU balance.

    CF placeholder = 0.0 until teammate integrates CF module.
    To integrate CF, replace cf_score = 0.0 with:
        cf_df     = get_cf_scores(bq_client, user_ids, songs_df, ...)
        cf_lookup = cf_df.set_index("video_id")["cf_score"]
        cf_score  = result["video_id"].map(lambda v: cf_lookup.get(v, 0.0))

    Returns:
        (recommendations DataFrame, gru_weight float)
    """
    if cbf_df.empty:
        logger.warning("CBF returned no results. Cannot aggregate.")
        return pd.DataFrame(), 0.0

    result   = cbf_df.copy()
    cf_score = 0.0   # ← CF placeholder, replace when teammate is ready

    # session_score comes from CBF.py (normalized Firestore feedback)
    # default to 0.0 if column missing (safety)
    if "session_score" not in result.columns:
        result["session_score"] = 0.0

    gru_weight = _resolve_gru_weight(songs_played, gru_active)

    if not gru_active or gru_df.empty or gru_weight == 0.0:
        # cold start: CBF + CF + session feedback nudge
        cbf_weight = WEIGHT_CBF_COLD / (WEIGHT_CBF_COLD + WEIGHT_CF_COLD)
        result["gru_score"]   = 0.0
        result["final_score"] = (
            cbf_weight       * result["cbf_score"] +
            (1 - cbf_weight) * cf_score +
            SESSION_SCORE_WEIGHT * result["session_score"]
        )
        logger.info(
            f"Cold start (songs_played={songs_played}) — "
            f"CBF: {cbf_weight:.2f}, CF: {1-cbf_weight:.2f}, GRU: 0.0, "
            f"session_score nudge: {SESSION_SCORE_WEIGHT}"
        )
    else:
        # warm: CBF + CF + GRU with normalized weights + session feedback nudge
        cbf_weight = WEIGHT_CBF_WARM
        cf_weight  = WEIGHT_CF_WARM
        total      = cbf_weight + cf_weight + gru_weight

        gru_lookup          = dict(zip(gru_df["video_id"], gru_df["gru_score"]))
        result["gru_score"] = result["video_id"].map(
            lambda vid: gru_lookup.get(vid, 0.0)
        )
        result["final_score"] = (
            (cbf_weight / total) * result["cbf_score"] +
            (cf_weight  / total) * cf_score +
            (gru_weight / total) * result["gru_score"] +
            SESSION_SCORE_WEIGHT * result["session_score"]
        )
        logger.info(
            f"Warm (songs_played={songs_played}) — "
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
    state.embedding_lookup = build_embedding_lookup(state.songs_df)
    logger.info(f"Loaded {len(state.songs_df)} songs from BigQuery")

    try:
        state.gru_model = load_gru_model()
    except Exception as e:
        logger.warning(
            f"GRU model could not be loaded: {e}. Using CBF only."
        )
        state.gru_model = None

    state.user_ids = get_session_user_ids(state.db, session_id)
    logger.info(f"Session users: {state.user_ids}")
    logger.info(f"Session {session_id} initialized.")
    return state


# ── Recommendation cycle ──────────────────────────────────────────────────────
def recommend_next_song(state: SessionState) -> dict:
    """
    Called every REFRESH_THRESHOLD songs by ml_trigger.py.

    Reads latest session state from Firestore + BigQuery,
    runs CBF + GRU (if active), aggregates with gradual
    GRU weight ramp, writes top N to Firestore.

    Returns top recommended song as dict.
    """
    session_id = state.session_id
    db         = state.db
    logger.info(f"Running recommendation cycle for session {session_id}")

    # ── read session state from Firestore ─────────────────────────────────────
    play_sequence  = get_session_play_sequence(db, session_id)
    track_scores   = get_session_track_scores(db, session_id)
    already_played = get_already_played_ids(play_sequence)

    # ── read user liked/disliked songs from BigQuery ──────────────────────────
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

    # ── build user vectors ────────────────────────────────────────────────────
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

    # ── CBF ───────────────────────────────────────────────────────────────────
    cbf_df = get_cbf_scores(
        global_vector=global_vector,
        songs_df=state.songs_df,
        user_liked_songs=user_liked,
        user_disliked_songs=user_disliked,
        already_played_ids=already_played,
        track_scores=track_scores,
        top_n=TOP_N,
    )

    # ── GRU ───────────────────────────────────────────────────────────────────
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

    # ── aggregate ─────────────────────────────────────────────────────────────
    recommendations, gru_weight = _aggregate_scores(
        cbf_df=cbf_df,
        gru_df=gru_df,
        gru_active=gru_active,
        songs_played=songs_played,
    )

    if recommendations.empty:
        logger.error("No recommendations generated.")
        return {}

    # ── write to Firestore ────────────────────────────────────────────────────
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


# ── Local testing ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m ml.recommendation.main <session_id>")
        sys.exit(1)

    session_id = sys.argv[1]
    state      = initialize_session(session_id)
    top_song   = recommend_next_song(state)
    print(f"\nTop recommendation: {top_song}")