"""
GRU inference module.

Reads the current session play sequence from Firestore,
runs it through the trained GRU model, and returns
ranked song scores compatible with CBF scores for
weighted aggregation in main.py.

Only activates after GRU_ACTIVATION_THRESHOLD songs have played.
"""

import logging
import numpy as np
import pandas as pd
import torch
from sklearn.metrics.pairwise import cosine_similarity

from ml.gru.gru_model import build_model
from ml.recommendation.config import (
    GRU_EMBEDDING_DIM,
    GRU_HIDDEN_DIM,
    GRU_NUM_LAYERS,
    GRU_MODEL_PATH,
    GRU_ACTIVATION_THRESHOLD,
    TOP_N,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_SEQ_LEN = 15


# ── Model loading ─────────────────────────────────────────────────────────────
def _load_state_dict(model_path: str) -> dict:
    """
    Loads model state dict from either local path or GCS.
    Handles both:
      local: ml/gru/gru_model.pt
      GCS:   gs://bucket/models/gru_model.pt
    """
    if model_path.startswith("gs://"):
        # download from GCS into memory
        from google.cloud import storage
        import io
        parts      = model_path[5:].split("/", 1)
        bucket_name, blob_path = parts[0], parts[1]
        client     = storage.Client()
        blob       = client.bucket(bucket_name).blob(blob_path)
        buffer     = io.BytesIO()
        blob.download_to_file(buffer)
        buffer.seek(0)
        return torch.load(buffer, map_location="cpu", weights_only=True)
    else:
        return torch.load(model_path, map_location="cpu", weights_only=True)


def load_gru_model(model_path: str = GRU_MODEL_PATH) -> torch.nn.Module:
    """
    Loads trained GRU model weights from disk or GCS.
    Called once at session start — model stays in memory.
    """
    model = build_model(
        embedding_dim=GRU_EMBEDDING_DIM,
        hidden_dim=GRU_HIDDEN_DIM,
        num_layers=GRU_NUM_LAYERS,
    )
    model.load_state_dict(_load_state_dict(model_path))
    model.eval()
    logger.info(f"GRU model loaded from {model_path}")
    return model


# ── Embedding lookup ──────────────────────────────────────────────────────────
def build_embedding_lookup(songs_df: pd.DataFrame) -> dict:
    """
    Builds video_id → np.array(386,) lookup from BigQuery songs DataFrame.
    Built once per session and reused across all recommendation cycles.
    """
    return {
        row["video_id"]: np.array(row["embedding"], dtype=np.float32)
        for _, row in songs_df.iterrows()
    }


# ── Sequence builder ──────────────────────────────────────────────────────────
def _build_sequence_tensor(
    play_sequence: list,
    embedding_lookup: dict,
) -> torch.Tensor:
    """
    Converts Firestore play sequence into GRU input tensor.

    play_sequence from firebase_client.get_session_play_sequence():
    [
        { "video_id": "abc", "play_order": 1, "liked_flag": 1 },
        { "video_id": "xyz", "play_order": 2, "liked_flag": 0 },
        ...
    ]

    Returns:
        tensor of shape (1, MAX_SEQ_LEN, 387)
        or None if sequence cannot be built
    """
    seq_tensors = []

    for event in play_sequence:
        video_id   = event["video_id"]
        liked_flag = event["liked_flag"]

        if video_id not in embedding_lookup:
            logger.warning(
                f"video_id '{video_id}' not found in embedding lookup. Skipping."
            )
            continue

        emb      = embedding_lookup[video_id]                      # (386,)
        flag     = np.array([liked_flag], dtype=np.float32)        # (1,)
        combined = np.concatenate([emb, flag])                     # (387,)
        seq_tensors.append(combined)

    if not seq_tensors:
        logger.warning("No valid events found in play sequence.")
        return None

    # left-pad with zeros to MAX_SEQ_LEN
    while len(seq_tensors) < MAX_SEQ_LEN:
        seq_tensors.insert(
            0, np.zeros(GRU_EMBEDDING_DIM + 1, dtype=np.float32)
        )
    seq_tensors = seq_tensors[-MAX_SEQ_LEN:]

    # shape: (1, MAX_SEQ_LEN, 387)
    return torch.tensor(
        np.stack(seq_tensors), dtype=torch.float32
    ).unsqueeze(0)


# ── GRU inference ─────────────────────────────────────────────────────────────
def get_gru_scores(
    model:              torch.nn.Module,
    play_sequence:      list,
    embedding_lookup:   dict,
    songs_df:           pd.DataFrame,
    already_played_ids: set,
    top_n:              int = TOP_N,
) -> pd.DataFrame:
    """
    Runs GRU inference on current session play sequence.
    Returns ranked candidate songs by GRU score.

    Only activates when len(play_sequence) >= GRU_ACTIVATION_THRESHOLD.

    Args:
        model:              loaded SessionGRU model
        play_sequence:      from firebase_client.get_session_play_sequence()
        embedding_lookup:   { video_id: np.array(386,) }
        songs_df:           all songs DataFrame from BigQuery
        already_played_ids: set of video_ids played this session
        top_n:              number of songs to return

    Returns:
        DataFrame with columns:
        video_id, track_title, artist_name, genre, gru_score
        Empty DataFrame if GRU not yet activated.
    """
    # ── activation check ──────────────────────────────────────────────────────
    if len(play_sequence) < GRU_ACTIVATION_THRESHOLD:
        logger.info(
            f"GRU not activated — "
            f"{len(play_sequence)}/{GRU_ACTIVATION_THRESHOLD} songs played."
        )
        return pd.DataFrame()

    # ── build input tensor ────────────────────────────────────────────────────
    x = _build_sequence_tensor(play_sequence, embedding_lookup)
    if x is None:
        logger.warning("Could not build sequence tensor. Skipping GRU.")
        return pd.DataFrame()

    # ── forward pass ──────────────────────────────────────────────────────────
    with torch.no_grad():
        predicted_embedding = model(x)                      # (1, 386)
        predicted_np        = predicted_embedding.numpy()   # (1, 386)

    logger.info(
        f"GRU inference done — sequence length: {len(play_sequence)}"
    )

    # ── exclude already played songs ─────────────────────────────────────────
    candidate_songs = songs_df[
        ~songs_df["video_id"].isin(already_played_ids)
    ].reset_index(drop=True)

    if candidate_songs.empty:
        logger.warning("No candidate songs available for GRU scoring.")
        return pd.DataFrame()

    # ── cosine similarity against full catalog ────────────────────────────────
    embedding_matrix         = np.stack(candidate_songs["embedding"].values)
    gru_scores               = cosine_similarity(predicted_np, embedding_matrix)[0]
    candidate_songs          = candidate_songs.copy()
    candidate_songs["gru_score"] = gru_scores

    # ── rank and return top N ─────────────────────────────────────────────────
    recommendations = (
        candidate_songs
        .sort_values("gru_score", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )

    logger.info(
        f"GRU top {top_n}: "
        f"'{recommendations.iloc[0]['track_title']}' "
        f"(gru_score={recommendations.iloc[0]['gru_score']:.4f})"
    )

    return recommendations[[
        "video_id", "track_title", "artist_name", "genre", "gru_score"
    ]]