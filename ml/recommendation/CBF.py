import logging

import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

from ml.recommendation.config import TOP_N

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# how strongly session feedback nudges CBF scores
SESSION_SCORE_WEIGHT = 0.3


def _normalize_session_scores(track_scores: dict) -> dict:
    """
    Normalizes raw Firestore session scores to [-1, +1] range.
    This prevents a single highly liked/disliked song from
    completely overriding CBF similarity scores.

    Example:
        raw scores: { "abc": 4.0, "xyz": -2.0, "def": 0.5 }
        normalized: { "abc": 1.0, "xyz": -0.5, "def": 0.125 }
    """
    if not track_scores:
        return {}

    scores = np.array([v["score"] for v in track_scores.values()])
    max_abs = np.abs(scores).max()

    if max_abs == 0:
        return {vid: 0.0 for vid in track_scores}

    return {
        vid: track_scores[vid]["score"] / max_abs
        for vid in track_scores
    }


def _build_exclusion_set(
    user_liked_songs: dict,
    user_disliked_songs: dict,
    already_played_ids: set,
) -> set:
    """
    Builds the set of video_ids to exclude from recommendations:
    1. Songs already liked by any user (they know it, no need to play)
    2. Songs explicitly disliked by any user
    3. Songs already played in this session
    """
    liked_ids    = {vid for ids in user_liked_songs.values()    for vid in ids}
    disliked_ids = {vid for ids in user_disliked_songs.values() for vid in ids}
    return liked_ids | disliked_ids | already_played_ids


def get_cbf_scores(
    global_vector: np.ndarray,
    songs_df: pd.DataFrame,
    user_liked_songs: dict,
    user_disliked_songs: dict,
    already_played_ids: set,
    track_scores: dict,
    top_n: int = TOP_N,
) -> pd.DataFrame:
    """
    Content-Based Filtering recommendation.

    Steps:
    1. Exclude already liked, disliked, and played songs
    2. Compute cosine similarity between global preference
       vector and all candidate song embeddings
    3. Nudge scores using live Firestore session feedback
    4. Return top N songs ranked by final score

    Args:
        global_vector:       (1, 386) group preference vector
        songs_df:            DataFrame with all song embeddings from BigQuery
        user_liked_songs:    { user_id: [liked video_ids] }
        user_disliked_songs: { user_id: [disliked video_ids] }
        already_played_ids:  set of video_ids played this session
        track_scores:        { video_id: { score, like_count, ... } }
                             from Firestore streaming pipeline
        top_n:               number of recommendations to return

    Returns:
        DataFrame with columns:
        video_id, track_title, artist_name, genre,
        cbf_score, session_score, final_score
    """
    # ── 1. build exclusion set ────────────────────────────────────────────────
    exclude_ids = _build_exclusion_set(
        user_liked_songs,
        user_disliked_songs,
        already_played_ids
    )

    candidate_songs = songs_df[
        ~songs_df["video_id"].isin(exclude_ids)
    ].reset_index(drop=True)

    if candidate_songs.empty:
        logger.warning("No candidate songs available for CBF recommendation.")
        return pd.DataFrame()

    logger.info(
        f"CBF: {len(candidate_songs)} candidates "
        f"({len(exclude_ids)} excluded)"
    )

    # ── 2. cosine similarity ──────────────────────────────────────────────────
    embedding_matrix = np.stack(candidate_songs["embedding"].values)
    cbf_scores       = cosine_similarity(global_vector, embedding_matrix)[0]

    candidate_songs          = candidate_songs.copy()
    candidate_songs["cbf_score"] = cbf_scores

    # ── 3. session score nudge ────────────────────────────────────────────────
    normalized_scores = _normalize_session_scores(track_scores)

    candidate_songs["session_score"] = candidate_songs["video_id"].map(
        lambda vid: normalized_scores.get(vid, 0.0)
    )

    # final_score = cbf_similarity + (normalized_session_score * weight)
    candidate_songs["final_score"] = (
        candidate_songs["cbf_score"] +
        candidate_songs["session_score"] * SESSION_SCORE_WEIGHT
    )

    # ── 4. rank and return top N ──────────────────────────────────────────────
    recommendations = (
        candidate_songs
        .sort_values("final_score", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )

    logger.info(
        f"CBF top {top_n} recommendations generated. "
        f"Top song: '{recommendations.iloc[0]['track_title']}' "
        f"(final_score={recommendations.iloc[0]['final_score']:.4f})"
    )

    return recommendations[[
        "video_id",
        "track_title",
        "artist_name",
        "genre",
        "cbf_score",
        "session_score",
        "final_score",
    ]]