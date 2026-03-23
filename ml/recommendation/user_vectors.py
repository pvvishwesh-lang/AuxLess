import logging

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_user_vectors(
    user_liked_songs: dict,
    user_disliked_songs: dict,
    songs_df: pd.DataFrame,
) -> dict:
    """
    Builds a preference vector per user by:
      1. Averaging embeddings of liked songs     → positive signal
      2. Averaging embeddings of disliked songs  → negative signal
      3. Subtracting negative from positive      → net preference vector

    Returns:
    {
        "user_1": {
            "vector":      np.array (386,),
            "liked_count": 5,        ← used for weighting global vector
        },
        ...
    }
    """
    user_vectors = {}

    for user_id, liked_ids in user_liked_songs.items():

        # ── positive signal ───────────────────────────────────────────────────
        liked_songs = songs_df[songs_df["video_id"].isin(liked_ids)]
        if liked_songs.empty:
            logger.warning(f"No liked songs found in catalog for {user_id}. Skipping.")
            continue

        positive_vector = np.mean(
            np.stack(liked_songs["embedding"].values), axis=0
        )

        # ── negative signal (if any disliked songs exist) ─────────────────────
        disliked_ids   = user_disliked_songs.get(user_id, [])
        disliked_songs = songs_df[songs_df["video_id"].isin(disliked_ids)]

        if not disliked_songs.empty:
            negative_vector = np.mean(
                np.stack(disliked_songs["embedding"].values), axis=0
            )
            # subtract negative signal → push away from disliked content
            net_vector = positive_vector - 0.5 * negative_vector
        else:
            net_vector = positive_vector

        # ── normalize to unit vector ──────────────────────────────────────────
        norm = np.linalg.norm(net_vector)
        if norm > 0:
            net_vector = net_vector / norm

        user_vectors[user_id] = {
            "vector":      net_vector,
            "liked_count": len(liked_songs),
        }

        logger.info(
            f"Built vector for {user_id} — "
            f"liked: {len(liked_songs)}, disliked: {len(disliked_songs)}"
        )

    return user_vectors


def build_global_vector(user_vectors: dict) -> np.ndarray:
    """
    Builds a single group preference vector by averaging
    all user vectors weighted by their liked song count.

    Users with more liked songs contribute more to the group vector
    because their preference signal is more reliable.

    Returns np.array of shape (1, 386)
    """
    if not user_vectors:
        raise ValueError("No user vectors available to build global vector.")

    vectors = np.stack([v["vector"]      for v in user_vectors.values()])
    weights = np.array([v["liked_count"] for v in user_vectors.values()],
                       dtype=float)

    # normalize weights so they sum to 1
    weights = weights / weights.sum()

    # weighted average
    global_vector = np.average(vectors, axis=0, weights=weights)

    # normalize to unit vector
    norm = np.linalg.norm(global_vector)
    if norm > 0:
        global_vector = global_vector / norm

    logger.info(
        f"Built global vector from {len(user_vectors)} users "
        f"(weights: { {uid: round(w, 3) for uid, w in zip(user_vectors.keys(), weights)} })"
    )

    return global_vector.reshape(1, -1)


def get_already_played_ids(play_sequence: list) -> set:
    """
    Extracts video_ids of songs already played in the session.
    These are excluded from recommendations so the same song
    never gets recommended twice in a session.

    play_sequence comes from firebase_client.get_session_play_sequence()
    [
        { "video_id": "abc", "play_order": 1, "liked_flag": 1 },
        { "video_id": "xyz", "play_order": 2, "liked_flag": 0 },
    ]
    """
    return {event["video_id"] for event in play_sequence}