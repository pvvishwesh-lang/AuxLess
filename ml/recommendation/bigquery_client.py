"""
BigQuery client for:
1. Song catalog — embeddings for CBF and GRU
2. User DB      — liked/disliked songs per user
                  stored as ARRAY<STRING> of video_ids

BigQuery tables:
  song_recommendations.song_embeddings  -> song catalog
  song_recommendations.users            -> user liked/disliked songs

All table references come from config.py.
"""

import logging
import numpy as np
import pandas as pd
from google.cloud import bigquery

from ml.recommendation.config import PROJECT_ID, TABLE_REF, USERS_TABLE_REF

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


# ── Song catalog ──────────────────────────────────────────────────────────────

def fetch_all_embeddings(client: bigquery.Client) -> pd.DataFrame:
    """
    Pulls all songs and their embedding vectors from BigQuery.
    Called once per session at initialize_session().
    """
    logger.info("Fetching all song embeddings from BigQuery...")
    query = f"""
        SELECT
            video_id,
            track_title,
            artist_name,
            genre,
            embedding
        FROM `{TABLE_REF}`
    """
    result = client.query(query).result()
    if hasattr(result, 'to_dataframe'):
        df = result.to_dataframe(dtypes={"embedding": object})
        df["embedding"] = df["embedding"].apply(lambda x: np.array(x, dtype=np.float32))
    else:
        rows = []
        for row in result:
            rows.append({
                "video_id":    row.video_id,
                "track_title": row.track_title,
                "artist_name": row.artist_name,
                "genre":       row.genre,
                "embedding":   np.array(row.embedding, dtype=np.float32),
            })
        df = pd.DataFrame(rows)
    logger.info(f"Fetched {len(df)} songs from BigQuery.")
    return df


def get_existing_video_ids(client: bigquery.Client) -> set:
    """
    Returns set of video_ids already in BigQuery song catalog.
    Used to skip re-embedding songs already present.
    """
    try:
        result = client.query(
            f"SELECT video_id FROM `{TABLE_REF}`"
        ).result()
        ids = {row.video_id for row in result}
        logger.info(f"{len(ids)} songs already in BigQuery.")
        return ids
    except Exception:
        logger.info("Song catalog is empty.")
        return set()


def load_embeddings(client: bigquery.Client, df: pd.DataFrame):
    """
    Inserts new song embeddings into BigQuery song catalog.
    Skips songs already present.
    """
    logger.info(f"Starting load -- {len(df)} songs in batch.")

    existing_ids = get_existing_video_ids(client)
    df_new       = df[~df["video_id"].isin(existing_ids)].reset_index(drop=True)

    logger.info(
        f"Skipped {len(df) - len(df_new)} duplicates. "
        f"{len(df_new)} new songs to insert."
    )

    if df_new.empty:
        logger.info("Nothing to insert.")
        return

    errors = client.insert_rows_json(TABLE_REF, df_new.to_dict(orient="records"))
    if errors:
        logger.error(f"BigQuery insert errors: {errors}")
    else:
        logger.info(f"Inserted {len(df_new)} songs into BigQuery.")


# ── User DB ───────────────────────────────────────────────────────────────────

def get_user_liked_songs(
    client:   bigquery.Client,
    user_ids: list,
) -> dict:
    """
    Fetches liked song video_ids for specific users.
    Used to build per-user preference vectors for CBF.
    """
    if not user_ids:
        return {}

    ids_str = ", ".join([f"'{uid}'" for uid in user_ids])
    query   = f"""
        SELECT
            user_id,
            liked_songs
        FROM `{USERS_TABLE_REF}`
        WHERE user_id IN ({ids_str})
    """

    result = {}
    try:
        for row in client.query(query).result():
            liked = list(row.liked_songs) if row.liked_songs else []
            if liked:
                result[row.user_id] = liked
                logger.info(
                    f"Fetched {len(liked)} liked songs "
                    f"for user {row.user_id}."
                )
            else:
                logger.warning(f"User {row.user_id} has no liked songs yet.")
    except Exception as e:
        logger.error(f"Failed to fetch liked songs from BigQuery: {e}")

    return result


def get_user_disliked_songs(
    client:   bigquery.Client,
    user_ids: list,
) -> dict:
    """
    Fetches disliked song video_ids for specific users.
    Used to push recommendations away from disliked content.
    """
    if not user_ids:
        return {}

    ids_str = ", ".join([f"'{uid}'" for uid in user_ids])
    query   = f"""
        SELECT
            user_id,
            disliked_songs
        FROM `{USERS_TABLE_REF}`
        WHERE user_id IN ({ids_str})
    """

    result = {}
    try:
        for row in client.query(query).result():
            disliked = list(row.disliked_songs) if row.disliked_songs else []
            if disliked:
                result[row.user_id] = disliked
                logger.info(
                    f"Fetched {len(disliked)} disliked songs "
                    f"for user {row.user_id}."
                )
    except Exception as e:
        logger.error(f"Failed to fetch disliked songs from BigQuery: {e}")

    return result


def fetch_all_user_liked(client: bigquery.Client) -> dict:
    """
    Fetches liked songs for ALL users in the database.
    Used by Collaborative Filtering to build the co-occurrence index.

    Returns:
        { user_id: [video_id, ...] }
    """
    query = f"""
        SELECT user_id, liked_songs
        FROM `{USERS_TABLE_REF}`
        WHERE ARRAY_LENGTH(liked_songs) > 0
    """

    result = {}
    try:
        for row in client.query(query).result():
            liked = list(row.liked_songs) if row.liked_songs else []
            if liked:
                result[row.user_id] = liked
        logger.info(f"Fetched liked songs for {len(result)} users (full population).")
    except Exception as e:
        logger.error(f"Failed to fetch all user liked songs: {e}")

    return result