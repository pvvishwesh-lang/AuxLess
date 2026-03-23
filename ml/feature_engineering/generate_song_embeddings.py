import io
import logging
import os

import numpy as np
import pandas as pd
from google.cloud import storage
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import MinMaxScaler

from ml.feature_engineering.bigquery_loader import get_client, load_embeddings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Embedding model (loaded once at module level) ─────────────────────────────
logger.info("Loading embedding model...")
_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


# ── GCS read ──────────────────────────────────────────────────────────────────
def _read_csv_from_gcs(bucket_name: str, blob_path: str) -> pd.DataFrame:
    client  = storage.Client()
    content = client.bucket(bucket_name).blob(blob_path).download_as_text()
    return pd.read_csv(io.StringIO(content))


# ── Text representation ───────────────────────────────────────────────────────
def build_song_text(row) -> str:
    """
    Combines song metadata into a single sentence for the text encoder.
    Richer text → better embedding separation between genres/artists.
    """
    return (
        f"title: {row['track_title']} "
        f"artist: {row['artist_name']} "
        f"genre: {row['genre']} "
        f"album: {row['collection_name']}"
    )


# ── Embedding generation ──────────────────────────────────────────────────────
def generate_embeddings(df: pd.DataFrame) -> np.ndarray:
    """
    Generates 386-dim embeddings per song:
      - 384 dims from MiniLM text encoder (title + artist + genre + album)
      - 2 dims from normalized numeric features (duration_sec, popularity_score)
    """
    logger.info(f"Generating embeddings for {len(df)} songs...")

    # text embeddings (384-dim)
    df["song_text"] = df.apply(build_song_text, axis=1)
    text_embeddings = _model.encode(
        df["song_text"].tolist(),
        show_progress_bar=True,
        batch_size=64
    )

    # numeric features (2-dim, normalized)
    scaler          = MinMaxScaler()
    numeric_scaled  = scaler.fit_transform(df[["duration_sec", "popularity_score"]])

    # concatenate → 386-dim final embedding
    final_embeddings = np.hstack([text_embeddings, numeric_scaled])
    logger.info(f"Embedding shape: {final_embeddings.shape}")
    return final_embeddings


# ── DataFrame builder ─────────────────────────────────────────────────────────
def build_embedding_dataframe(df: pd.DataFrame, embeddings: np.ndarray) -> pd.DataFrame:
    """
    Builds the DataFrame that gets uploaded to BigQuery.
    Stores the full 386-dim vector as a single list per row.
    """
    result = df[[
        "video_id",
        "track_title",
        "artist_name",
        "genre",
        "duration_sec",
        "popularity_score",
    ]].reset_index(drop=True).copy()

    result["embedding"] = [emb.tolist() for emb in embeddings]
    return result


# ── Session-scoped entry point ────────────────────────────────────────────────
def generate_embeddings_for_session(session_id: str, bucket: str) -> int:
    """
    Triggered after preprocess_for_session() completes.
    Reads the preprocessed CSV from GCS, generates embeddings
    for new songs only, and uploads them to BigQuery.

    Returns the number of new songs inserted into BigQuery.
    """
    blob_path = f"ml_output/{session_id}_preprocessed.csv"

    logger.info(f"Loading preprocessed songs: gs://{bucket}/{blob_path}")
    df = _read_csv_from_gcs(bucket, blob_path)
    logger.info(f"Loaded {len(df)} songs for session {session_id}")

    if df.empty:
        logger.warning(f"No songs found for session {session_id}. Skipping.")
        return 0

    # fetch existing video_ids from BigQuery to skip duplicates
    bq_client       = get_client()
    existing_ids    = _get_existing_ids(bq_client)
    df_new          = df[~df["video_id"].isin(existing_ids)].reset_index(drop=True)

    logger.info(
        f"Skipping {len(df) - len(df_new)} existing songs. "
        f"{len(df_new)} new songs to embed."
    )

    if df_new.empty:
        logger.info("All songs already in BigQuery. Nothing to embed.")
        return 0

    embeddings    = generate_embeddings(df_new)
    embedding_df  = build_embedding_dataframe(df_new, embeddings)

    load_embeddings(bq_client, embedding_df)
    logger.info(f"Uploaded {len(embedding_df)} new songs to BigQuery.")

    return len(embedding_df)


def _get_existing_ids(bq_client) -> set:
    """
    Pulls existing video_ids from BigQuery.
    Separated from bigquery_loader to avoid a double client instantiation.
    """
    from ml.feature_engineering.bigquery_loader import get_existing_video_ids
    return get_existing_video_ids(bq_client)


# ── Local dev / testing entry point ──────────────────────────────────────────
if __name__ == "__main__":
    import sys

    LOCAL_INPUT = "../preprocessing/data/preprocessed_songs.csv"

    if not os.path.exists(LOCAL_INPUT):
        print(f"File not found: {LOCAL_INPUT}")
        sys.exit(1)

    df         = pd.read_csv(LOCAL_INPUT)
    embeddings = generate_embeddings(df)
    emb_df     = build_embedding_dataframe(df, embeddings)

    print(f"Embedding shape: {embeddings.shape}")
    print(f"Sample (first 5 dims): {emb_df['embedding'][0][:5]}")

    client = get_client()
    load_embeddings(client, emb_df)