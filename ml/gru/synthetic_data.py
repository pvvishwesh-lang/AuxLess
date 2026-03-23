"""
Generates genre-aware synthetic session data for GRU training.

Fetches the full song catalog from BigQuery (82K songs) and
generates realistic session sequences where:
- Each session has a preferred genre
- Songs matching preferred genre are liked 85% of the time
- Songs not matching are liked only 20% of the time

This creates realistic sequential patterns for the GRU to learn.
"""

import logging
import uuid
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Like probabilities ────────────────────────────────────────────────────────
LIKE_PROB_MATCH     = 0.85   # song matches session preferred genre
LIKE_PROB_NO_MATCH  = 0.20   # song does not match preferred genre

# ── Session parameters ────────────────────────────────────────────────────────
MIN_SONGS_PER_SESSION = 5
MAX_SONGS_PER_SESSION = 15


def generate_synthetic_sessions(
    songs_df: pd.DataFrame,
    num_sessions: int = 5000,
    max_users: int = 6,
    random_seed: int = 42,
) -> pd.DataFrame:
    """
    Generates genre-aware synthetic session data using
    the full BigQuery song catalog.

    Args:
        songs_df:     DataFrame fetched from BigQuery
                      must have: video_id, track_title, artist_name, genre
        num_sessions: number of synthetic sessions to generate
        max_users:    max users per session
        random_seed:  for reproducibility

    Returns DataFrame with columns:
        session_id, preferred_genre, video_id, track_title,
        artist_name, genre, play_order, liked_flag,
        timestamp, num_users
    """
    np.random.seed(random_seed)

    # get unique genres from catalog
    genres    = songs_df["genre"].dropna().unique().tolist()
    genres    = [g for g in genres if g and g != "unknown"]
    if not genres:
        genres = ["unknown"]

    logger.info(
        f"Generating {num_sessions} synthetic sessions "
        f"from {len(songs_df)} songs across {len(genres)} genres..."
    )

    rows = []

    for _ in range(num_sessions):
        session_id      = str(uuid.uuid4())
        num_users       = np.random.randint(1, max_users + 1)
        preferred_genre = np.random.choice(genres)
        start_time      = datetime.now()
        num_songs       = np.random.randint(
            MIN_SONGS_PER_SESSION,
            MAX_SONGS_PER_SESSION + 1
        )

        # sample songs without replacement
        # bias sampling toward preferred genre (60% preferred, 40% other)
        preferred_songs = songs_df[songs_df["genre"] == preferred_genre]
        other_songs     = songs_df[songs_df["genre"] != preferred_genre]

        n_preferred = min(int(num_songs * 0.6), len(preferred_songs))
        n_other     = min(num_songs - n_preferred, len(other_songs))

        sampled_songs = pd.concat([
            preferred_songs.sample(
                n=n_preferred,
                replace=False,
                random_state=np.random.randint(0, 100000)
            ) if n_preferred > 0 else pd.DataFrame(),
            other_songs.sample(
                n=n_other,
                replace=False,
                random_state=np.random.randint(0, 100000)
            ) if n_other > 0 else pd.DataFrame(),
        ]).sample(frac=1).reset_index(drop=True)  # shuffle order

        for order, (_, song) in enumerate(sampled_songs.iterrows()):
            # genre-aware liked_flag
            if song["genre"] == preferred_genre:
                liked_flag = int(np.random.choice(
                    [1, 0],
                    p=[LIKE_PROB_MATCH, 1 - LIKE_PROB_MATCH]
                ))
            else:
                liked_flag = int(np.random.choice(
                    [1, 0],
                    p=[LIKE_PROB_NO_MATCH, 1 - LIKE_PROB_NO_MATCH]
                ))

            rows.append({
                "session_id":      session_id,
                "preferred_genre": preferred_genre,
                "video_id":        song["video_id"],
                "track_title":     song["track_title"],
                "artist_name":     song["artist_name"],
                "genre":           song["genre"],
                "play_order":      order + 1,
                "liked_flag":      liked_flag,
                "timestamp":       start_time + timedelta(seconds=order * 180),
                "num_users":       num_users,
            })

    df = pd.DataFrame(rows)

    logger.info(
        f"Generated {num_sessions} sessions, "
        f"{len(df)} total song events. "
        f"Overall like rate: {df['liked_flag'].mean():.2%}"
    )
    return df


def get_session_sequences(df: pd.DataFrame) -> list:
    """
    Converts flat DataFrame into list of session sequences.
    Each sequence is a list of (video_id, liked_flag) tuples
    ordered by play_order.

    Used directly as input to GRU training.

    Returns:
    [
        [("video_id_1", 1), ("video_id_2", 0), ...],  # session 1
        [("video_id_3", 1), ("video_id_4", 1), ...],  # session 2
        ...
    ]
    """
    sequences = []
    for _, group in df.groupby("session_id"):
        group = group.sort_values("play_order")
        seq   = list(zip(group["video_id"], group["liked_flag"]))
        if len(seq) >= 2:   # need at least 2 songs for input → target
            sequences.append(seq)
    return sequences


if __name__ == "__main__":
    from ml.recommendation.bigquery_client import get_client, fetch_all_embeddings

    logger.info("Fetching songs from BigQuery...")
    client   = get_client()
    songs_df = fetch_all_embeddings(client)

    df = generate_synthetic_sessions(songs_df, num_sessions=5000)
    print(f"\nTotal rows: {len(df)}")
    print(f"Unique sessions: {df['session_id'].nunique()}")
    print(f"\nGenre distribution of preferred genres:")
    print(df.groupby("session_id")["preferred_genre"].first().value_counts().head(10))
    print(f"\nLike rate by genre match:")
    df["genre_match"] = df["genre"] == df["preferred_genre"]
    print(df.groupby("genre_match")["liked_flag"].mean())
    print(f"\nSample session:")
    print(df[df["session_id"] == df["session_id"].iloc[0]][[
        "play_order", "track_title", "genre", "liked_flag"
    ]].to_string())