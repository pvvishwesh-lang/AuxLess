"""
Training script for SessionGRU.

Training strategy:
- Fetch 82K song embeddings from BigQuery
- Generate 5000 genre-aware synthetic sessions
- For each session [s1, s2, s3, ..., sN] create multiple samples:
    input=[s1],       target=embedding(s2)
    input=[s1,s2],    target=embedding(s3)
    ...
- Loss: CosineEmbeddingLoss
- Early stopping + learning rate scheduling
- Saves best model weights to GRU_MODEL_PATH
"""

import os
import logging
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from sklearn.model_selection import train_test_split

from ml.gru.synthetic_data import generate_synthetic_sessions, get_session_sequences
from ml.gru.gru_model import build_model
from ml.recommendation.bigquery_client import get_client, fetch_all_embeddings
from ml.recommendation.config import (
    GRU_EMBEDDING_DIM,
    GRU_HIDDEN_DIM,
    GRU_NUM_LAYERS,
    GRU_MODEL_PATH,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_SEQ_LEN = 15   # matches max songs per session


# ── Dataset ───────────────────────────────────────────────────────────────────
class SessionDataset(Dataset):
    """
    Converts session sequences into (input_tensor, target_embedding) pairs.

    For session [s1, s2, s3, s4, s5] creates:
      ([s1],          target=emb(s2))
      ([s1, s2],      target=emb(s3))
      ([s1, s2, s3],  target=emb(s4))
      ([s1, s2, s3, s4], target=emb(s5))

    Training on all subsequences → better generalization.
    """

    def __init__(
        self,
        sequences:        list,
        embedding_lookup: dict,
    ):
        self.samples          = []
        self.embedding_lookup = embedding_lookup

        for seq in sequences:
            if len(seq) < 2:
                continue
            for i in range(1, len(seq)):
                input_seq        = seq[:i]
                target_video_id  = seq[i][0]

                # skip if any song missing from embedding lookup
                if target_video_id not in embedding_lookup:
                    continue
                if any(vid not in embedding_lookup for vid, _ in input_seq):
                    continue

                self.samples.append((input_seq, target_video_id))

        logger.info(f"Dataset: {len(self.samples)} training samples")

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        input_seq, target_video_id = self.samples[idx]

        # build sequence tensor: (MAX_SEQ_LEN, 387)
        seq_tensors = []
        for video_id, liked_flag in input_seq:
            emb      = self.embedding_lookup[video_id]           # (386,)
            flag     = np.array([liked_flag], dtype=np.float32)  # (1,)
            combined = np.concatenate([emb, flag])               # (387,)
            seq_tensors.append(combined)

        # left-pad with zeros to MAX_SEQ_LEN
        while len(seq_tensors) < MAX_SEQ_LEN:
            seq_tensors.insert(
                0, np.zeros(GRU_EMBEDDING_DIM + 1, dtype=np.float32)
            )
        seq_tensors = seq_tensors[-MAX_SEQ_LEN:]

        x      = torch.tensor(np.stack(seq_tensors), dtype=torch.float32)
        target = torch.tensor(
            self.embedding_lookup[target_video_id], dtype=torch.float32
        )
        return x, target


# ── Training ──────────────────────────────────────────────────────────────────
def train(
    num_sessions: int   = 5000,
    epochs:       int   = 50,
    batch_size:   int   = 64,
    lr:           float = 1e-3,
    val_split:    float = 0.1,
    patience:     int   = 7,
):
    """
    Full training pipeline:
    1. Fetch 82K song embeddings from BigQuery
    2. Generate 5000 genre-aware synthetic sessions
    3. Build train/val datasets
    4. Train GRU with CosineEmbeddingLoss
    5. Save best model
    """
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info(f"Training on: {device}")

    # ── 1. fetch embeddings from BigQuery ─────────────────────────────────────
    logger.info("Fetching song embeddings from BigQuery...")
    bq_client = get_client()
    songs_df  = fetch_all_embeddings(bq_client)
    logger.info(f"Loaded {len(songs_df)} songs from BigQuery")

    embedding_lookup = {
        row["video_id"]: np.array(row["embedding"], dtype=np.float32)
        for _, row in songs_df.iterrows()
    }

    # ── 2. generate synthetic sessions ───────────────────────────────────────
    logger.info(f"Generating {num_sessions} synthetic sessions...")
    synthetic_df = generate_synthetic_sessions(
        songs_df=songs_df,
        num_sessions=num_sessions
    )
    sequences = get_session_sequences(synthetic_df)
    logger.info(f"Got {len(sequences)} valid sequences")

    # ── 3. train/val split ────────────────────────────────────────────────────
    train_seqs, val_seqs = train_test_split(
        sequences, test_size=val_split, random_state=42
    )
    train_dataset = SessionDataset(train_seqs, embedding_lookup)
    val_dataset   = SessionDataset(val_seqs,   embedding_lookup)
    train_loader  = DataLoader(
        train_dataset, batch_size=batch_size, shuffle=True,  num_workers=2
    )
    val_loader    = DataLoader(
        val_dataset,   batch_size=batch_size, shuffle=False, num_workers=2
    )
    logger.info(
        f"Train: {len(train_dataset)} samples | "
        f"Val: {len(val_dataset)} samples"
    )

    # ── 4. model, optimizer, loss ─────────────────────────────────────────────
    model = build_model(
        embedding_dim=GRU_EMBEDDING_DIM,
        hidden_dim=GRU_HIDDEN_DIM,
        num_layers=GRU_NUM_LAYERS,
    ).to(device)

    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-4)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode="min", patience=3, factor=0.5
    )
    criterion = nn.CosineEmbeddingLoss()

    # ── 5. training loop ──────────────────────────────────────────────────────
    best_val_loss  = float("inf")
    patience_count = 0

    for epoch in range(epochs):

        # train
        model.train()
        train_loss = 0.0
        for x, target in train_loader:
            x, target = x.to(device), target.to(device)
            labels    = torch.ones(x.size(0), device=device)

            optimizer.zero_grad()
            predicted = model(x)
            loss      = criterion(predicted, target, labels)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            train_loss += loss.item()

        train_loss /= len(train_loader)

        # validate
        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for x, target in val_loader:
                x, target = x.to(device), target.to(device)
                labels    = torch.ones(x.size(0), device=device)
                predicted = model(x)
                loss      = criterion(predicted, target, labels)
                val_loss += loss.item()

        val_loss /= len(val_loader)
        scheduler.step(val_loss)

        logger.info(
            f"Epoch [{epoch+1:3d}/{epochs}] "
            f"train_loss: {train_loss:.4f} | "
            f"val_loss: {val_loss:.4f}"
        )

        # save best + early stopping
        if val_loss < best_val_loss:
            best_val_loss  = val_loss
            patience_count = 0
            os.makedirs(os.path.dirname(GRU_MODEL_PATH), exist_ok=True)
            torch.save(model.state_dict(), GRU_MODEL_PATH)
            logger.info(f"  ✓ Best model saved → {GRU_MODEL_PATH}")
        else:
            patience_count += 1
            logger.info(
                f"  No improvement ({patience_count}/{patience})"
            )
            if patience_count >= patience:
                logger.info(f"Early stopping at epoch {epoch+1}")
                break

    logger.info(
        f"Training complete. "
        f"Best val loss: {best_val_loss:.4f}"
    )
    return model


if __name__ == "__main__":
    train()