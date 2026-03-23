import io
import logging
import re

import numpy as np
import pandas as pd
from google.cloud import storage
from sklearn.preprocessing import MinMaxScaler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── GCS paths ────────────────────────────────────────────────────────────────
def _mitigated_path(session_id: str) -> str:
    return f"Final_Output/{session_id}_mitigated.csv"

def _preprocessed_path(session_id: str) -> str:
    return f"ml_output/{session_id}_preprocessed.csv"


# ── GCS read / write ─────────────────────────────────────────────────────────
def _read_csv_from_gcs(bucket_name: str, blob_path: str) -> pd.DataFrame:
    client  = storage.Client()
    content = client.bucket(bucket_name).blob(blob_path).download_as_text()
    return pd.read_csv(io.StringIO(content))

def _write_csv_to_gcs(df: pd.DataFrame, bucket_name: str, blob_path: str):
    client = storage.Client()
    client.bucket(bucket_name).blob(blob_path).upload_from_string(
        df.to_csv(index=False),
        content_type="text/csv"
    )
    logger.info(f"Saved preprocessed data to gs://{bucket_name}/{blob_path}")


# ── Text cleaning ─────────────────────────────────────────────────────────────
def clean_text(text) -> str:
    if pd.isnull(text):
        return "unknown"
    text = str(text).lower()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    return text.strip()


# ── Core preprocessing ────────────────────────────────────────────────────────
def preprocess_songs(df: pd.DataFrame) -> pd.DataFrame:
    # 1. deduplicate
    before = len(df)
    df = df.drop_duplicates(subset=["video_id"])
    logger.info(f"Deduplication: {before} → {len(df)} songs")

    # 2. early return if empty after dedup
    if df.empty:
        logger.warning("DataFrame is empty after deduplication. Returning empty.")
        return df

    # 3. fill missing values
    df["genre"]           = df["genre"].fillna("unknown")
    df["country"]         = df["country"].fillna("unknown")
    df["artist_name"]     = df["artist_name"].fillna("unknown")
    df["collection_name"] = df["collection_name"].fillna("unknown")

    # 4. clean text columns
    for col in ["track_title", "artist_name", "genre", "collection_name", "country"]:
        df[col] = df[col].apply(clean_text)

    # 5. convert numeric columns safely
    numeric_cols = [
        "trackTimeMillis", "trackTimeSeconds",
        "view_count", "like_count", "comment_count",
        "like_to_view_ratio", "comment_to_view_ratio"
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 6. duration in seconds
    df["duration_sec"] = df["trackTimeMillis"] / 1000

    # 7. recompute ratios if missing or zero
    df["like_to_view_ratio"] = np.where(
        df["like_to_view_ratio"] == 0,
        df["like_count"] / df["view_count"].replace(0, 1),
        df["like_to_view_ratio"]
    )
    df["comment_to_view_ratio"] = np.where(
        df["comment_to_view_ratio"] == 0,
        df["comment_count"] / df["view_count"].replace(0, 1),
        df["comment_to_view_ratio"]
    )

    # 8. popularity score
    pop_cols = ["view_count", "like_count", "comment_count"]
    scaler   = MinMaxScaler()
    scaled   = pd.DataFrame(
        scaler.fit_transform(df[pop_cols]),
        columns=pop_cols
    )
    df["popularity_score"] = (
        scaled["view_count"]    * 0.5 +
        scaled["like_count"]    * 0.3 +
        scaled["comment_count"] * 0.2
    )

    logger.info(f"Preprocessing complete. {len(df)} songs ready.")
    return df


# ── Session-scoped entry point ────────────────────────────────────────────────
def preprocess_for_session(session_id: str, bucket: str) -> str:
    """
    Triggered after batch pipeline completes (publish_session_ready).
    Reads the mitigated CSV from GCS, preprocesses it,
    and saves the result back to GCS.

    Returns the GCS path of the preprocessed CSV.
    """
    input_path  = _mitigated_path(session_id)
    output_path = _preprocessed_path(session_id)

    logger.info(f"Loading mitigated CSV: gs://{bucket}/{input_path}")
    df = _read_csv_from_gcs(bucket, input_path)
    logger.info(f"Loaded {len(df)} songs for session {session_id}")

    df_preprocessed = preprocess_songs(df)

    _write_csv_to_gcs(df_preprocessed, bucket, output_path)

    return f"gs://{bucket}/{output_path}"


# ── Local dev / testing entry point ──────────────────────────────────────────
if __name__ == "__main__":
    import os
    import sys

    LOCAL_INPUT  = "data/playlists/Pipeline_output.csv"
    LOCAL_OUTPUT = "data/preprocessed_songs.csv"

    if not os.path.exists(LOCAL_INPUT):
        print(f"File not found: {LOCAL_INPUT}")
        sys.exit(1)

    df  = pd.read_csv(LOCAL_INPUT)
    out = preprocess_songs(df)
    out.to_csv(LOCAL_OUTPUT, index=False)
    print(f"Saved {len(out)} songs to {LOCAL_OUTPUT}")