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
- Hyperparameter tuning with Optuna
- Experiment tracking with MLflow
- Saves best model weights to GRU_MODEL_PATH
"""

import os
import logging
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from sklearn.model_selection import train_test_split

import mlflow
import mlflow.pytorch
import optuna

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

# ── MLflow config ─────────────────────────────────────────────────────────────
MLFLOW_EXPERIMENT_NAME = "auxless-gru-training"


# ── Dataset ───────────────────────────────────────────────────────────────────
class SessionDataset(Dataset):
    """
    Converts session sequences into (input_tensor, target_embedding) pairs.

    For session [s1, s2, s3, s4, s5] creates:
      ([s1],          target=emb(s2))
      ([s1, s2],      target=emb(s3))
      ([s1, s2, s3],  target=emb(s4))
      ([s1, s2, s3, s4], target=emb(s5))

    Training on all subsequences -> better generalization.
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


# ── Data loading (shared across trials) ───────────────────────────────────────
def load_training_data(num_sessions: int = 5000, val_split: float = 0.1):
    """
    Fetches embeddings from BigQuery, generates synthetic sessions,
    and returns train/val sequences + embedding lookup.

    Called once and reused across all Optuna trials to avoid
    redundant BigQuery calls.
    """
    logger.info("Fetching song embeddings from BigQuery...")
    bq_client = get_client()
    songs_df  = fetch_all_embeddings(bq_client)
    logger.info(f"Loaded {len(songs_df)} songs from BigQuery")

    embedding_lookup = {
        row["video_id"]: np.array(row["embedding"], dtype=np.float32)
        for _, row in songs_df.iterrows()
    }

    logger.info(f"Generating {num_sessions} synthetic sessions...")
    synthetic_df = generate_synthetic_sessions(
        songs_df=songs_df,
        num_sessions=num_sessions
    )
    sequences = get_session_sequences(synthetic_df)
    logger.info(f"Got {len(sequences)} valid sequences")

    train_seqs, val_seqs = train_test_split(
        sequences, test_size=val_split, random_state=42
    )

    return train_seqs, val_seqs, embedding_lookup, songs_df


# ── Single training run ───────────────────────────────────────────────────────
def train(
    train_seqs:       list,
    val_seqs:         list,
    embedding_lookup: dict,
    hidden_dim:       int   = GRU_HIDDEN_DIM,
    num_layers:       int   = GRU_NUM_LAYERS,
    dropout:          float = 0.3,
    epochs:           int   = 50,
    batch_size:       int   = 64,
    lr:               float = 1e-3,
    patience:         int   = 7,
    save_model:       bool  = True,
    trial:            optuna.trial.Trial = None,
):
    """
    Trains a single GRU model with given hyperparameters.
    Logs all metrics and params to MLflow.

    Args:
        train_seqs:       list of training session sequences
        val_seqs:         list of validation session sequences
        embedding_lookup: {video_id: np.array(386,)}
        hidden_dim:       GRU hidden dimension
        num_layers:       number of GRU layers
        dropout:          dropout rate
        epochs:           max training epochs
        batch_size:       training batch size
        lr:               learning rate
        patience:         early stopping patience
        save_model:       whether to save best model to disk
        trial:            Optuna trial for pruning (None if standalone)

    Returns:
        (model, best_val_loss)
    """
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info(f"Training on: {device}")

    # ── build datasets ────────────────────────────────────────────────────────
    train_dataset = SessionDataset(train_seqs, embedding_lookup)
    val_dataset   = SessionDataset(val_seqs,   embedding_lookup)
    train_loader  = DataLoader(
        train_dataset, batch_size=batch_size, shuffle=True, num_workers=2
    )
    val_loader = DataLoader(
        val_dataset, batch_size=batch_size, shuffle=False, num_workers=2
    )
    logger.info(
        f"Train: {len(train_dataset)} samples | "
        f"Val: {len(val_dataset)} samples"
    )

    # ── model, optimizer, loss ────────────────────────────────────────────────
    model = build_model(
        embedding_dim=GRU_EMBEDDING_DIM,
        hidden_dim=hidden_dim,
        num_layers=num_layers,
        dropout=dropout,
    ).to(device)

    total_params = sum(p.numel() for p in model.parameters())

    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-4)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode="min", patience=3, factor=0.5
    )
    criterion = nn.CosineEmbeddingLoss()

    # ── log hyperparameters to MLflow ─────────────────────────────────────────
    mlflow.log_params({
        "hidden_dim":    hidden_dim,
        "num_layers":    num_layers,
        "dropout":       dropout,
        "batch_size":    batch_size,
        "learning_rate": lr,
        "patience":      patience,
        "max_epochs":    epochs,
        "max_seq_len":   MAX_SEQ_LEN,
        "embedding_dim": GRU_EMBEDDING_DIM,
        "optimizer":     "AdamW",
        "weight_decay":  1e-4,
        "scheduler":     "ReduceLROnPlateau",
        "loss_function": "CosineEmbeddingLoss",
        "total_params":  total_params,
        "train_samples": len(train_dataset),
        "val_samples":   len(val_dataset),
        "device":        str(device),
    })

    # ── training loop ─────────────────────────────────────────────────────────
    best_val_loss  = float("inf")
    patience_count = 0
    best_model_state = None

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

        current_lr = optimizer.param_groups[0]["lr"]

        # log metrics to MLflow
        mlflow.log_metrics({
            "train_loss":    train_loss,
            "val_loss":      val_loss,
            "learning_rate": current_lr,
        }, step=epoch)

        logger.info(
            f"Epoch [{epoch+1:3d}/{epochs}] "
            f"train_loss: {train_loss:.4f} | "
            f"val_loss: {val_loss:.4f} | "
            f"lr: {current_lr:.6f}"
        )

        # Optuna pruning — stop bad trials early
        if trial is not None:
            trial.report(val_loss, epoch)
            if trial.should_prune():
                logger.info(f"Trial pruned at epoch {epoch+1}")
                raise optuna.exceptions.TrialPruned()

        # save best + early stopping
        if val_loss < best_val_loss:
            best_val_loss    = val_loss
            patience_count   = 0
            best_model_state = model.state_dict().copy()

            if save_model:
                os.makedirs(os.path.dirname(GRU_MODEL_PATH) or ".", exist_ok=True)
                torch.save(model.state_dict(), GRU_MODEL_PATH)
                logger.info(f"  Best model saved -> {GRU_MODEL_PATH}")
        else:
            patience_count += 1
            logger.info(
                f"  No improvement ({patience_count}/{patience})"
            )
            if patience_count >= patience:
                logger.info(f"Early stopping at epoch {epoch+1}")
                break

    # log final metrics
    mlflow.log_metrics({
        "best_val_loss":   best_val_loss,
        "total_epochs":    epoch + 1,
        "early_stopped":   1 if patience_count >= patience else 0,
    })

    # log model artifact to MLflow
    if save_model and best_model_state is not None:
        model.load_state_dict(best_model_state)
        mlflow.pytorch.log_model(model, "gru_model")
        mlflow.log_artifact(GRU_MODEL_PATH, "model_weights")
        logger.info("Model artifact logged to MLflow")

    logger.info(
        f"Training complete. "
        f"Best val loss: {best_val_loss:.4f}"
    )
    return model, best_val_loss


# ── Optuna objective ──────────────────────────────────────────────────────────
def optuna_objective(trial, train_seqs, val_seqs, embedding_lookup):
    """
    Optuna objective function. Each trial samples hyperparameters,
    trains a model, and returns the best validation loss.

    Search space:
        hidden_dim:  128, 256, 512
        num_layers:  1, 2, 3
        dropout:     0.1 - 0.5
        lr:          1e-4 - 1e-2 (log scale)
        batch_size:  32, 64, 128
    """
    # sample hyperparameters
    hidden_dim = trial.suggest_categorical("hidden_dim", [128, 256, 512])
    num_layers = trial.suggest_int("num_layers", 1, 3)
    dropout    = trial.suggest_float("dropout", 0.1, 0.5, step=0.1)
    lr         = trial.suggest_float("lr", 1e-4, 1e-2, log=True)
    batch_size = trial.suggest_categorical("batch_size", [32, 64, 128])

    # each Optuna trial gets its own MLflow run
    with mlflow.start_run(nested=True, run_name=f"trial_{trial.number}"):
        mlflow.set_tag("trial_number", trial.number)
        mlflow.set_tag("run_type", "optuna_trial")

        _, best_val_loss = train(
            train_seqs=train_seqs,
            val_seqs=val_seqs,
            embedding_lookup=embedding_lookup,
            hidden_dim=hidden_dim,
            num_layers=num_layers,
            dropout=dropout,
            lr=lr,
            batch_size=batch_size,
            epochs=50,
            patience=7,
            save_model=False,   # only save the final best model
            trial=trial,
        )

    return best_val_loss


# ── Hyperparameter tuning with Optuna ─────────────────────────────────────────
def tune_and_train(
    num_sessions:  int = 5000,
    n_trials:      int = 20,
    val_split:     float = 0.1,
):
    """
    Full training pipeline with hyperparameter tuning:
    1. Load data from BigQuery (once)
    2. Run Optuna study (n_trials)
    3. Retrain final model with best hyperparameters
    4. Save best model to GRU_MODEL_PATH
    5. Log everything to MLflow
    """
    # ── setup MLflow ──────────────────────────────────────────────────────────
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    # ── load data once ────────────────────────────────────────────────────────
    train_seqs, val_seqs, embedding_lookup, songs_df = load_training_data(
        num_sessions=num_sessions,
        val_split=val_split,
    )

    # ── Optuna study ──────────────────────────────────────────────────────────
    with mlflow.start_run(run_name="optuna_study") as parent_run:
        mlflow.set_tag("run_type", "optuna_study")
        mlflow.log_params({
            "num_sessions":    num_sessions,
            "n_trials":        n_trials,
            "val_split":       val_split,
            "catalog_size":    len(songs_df),
            "train_sequences": len(train_seqs),
            "val_sequences":   len(val_seqs),
        })

        study = optuna.create_study(
            direction="minimize",
            study_name="auxless-gru-hpo",
            pruner=optuna.pruners.MedianPruner(
                n_startup_trials=5,
                n_warmup_steps=5,
            ),
        )

        study.optimize(
            lambda trial: optuna_objective(
                trial, train_seqs, val_seqs, embedding_lookup
            ),
            n_trials=n_trials,
            show_progress_bar=True,
        )

        # ── log Optuna results ────────────────────────────────────────────────
        best_params = study.best_params
        logger.info(f"Best hyperparameters: {best_params}")
        logger.info(f"Best val loss: {study.best_value:.4f}")

        mlflow.log_params({f"best_{k}": v for k, v in best_params.items()})
        mlflow.log_metric("best_trial_val_loss", study.best_value)
        mlflow.log_metric("total_trials", len(study.trials))
        mlflow.log_metric("pruned_trials",
            len([t for t in study.trials
                 if t.state == optuna.trial.TrialState.PRUNED]))

    # ── retrain with best params ──────────────────────────────────────────────
    logger.info("Retraining final model with best hyperparameters...")

    with mlflow.start_run(run_name="final_best_model"):
        mlflow.set_tag("run_type", "final_model")
        mlflow.set_tag("best_trial_number", study.best_trial.number)

        model, best_val_loss = train(
            train_seqs=train_seqs,
            val_seqs=val_seqs,
            embedding_lookup=embedding_lookup,
            hidden_dim=best_params["hidden_dim"],
            num_layers=best_params["num_layers"],
            dropout=best_params["dropout"],
            lr=best_params["lr"],
            batch_size=best_params["batch_size"],
            epochs=50,
            patience=7,
            save_model=True,
        )

    logger.info(
        f"Final model trained. "
        f"Best val loss: {best_val_loss:.4f} "
        f"Saved to: {GRU_MODEL_PATH}"
    )
    return model


# ── Standalone training (no Optuna) ───────────────────────────────────────────
def train_standalone(
    num_sessions: int   = 5000,
    epochs:       int   = 50,
    batch_size:   int   = 64,
    lr:           float = 1e-3,
    patience:     int   = 7,
):
    """
    Single training run without hyperparameter tuning.
    Still logs everything to MLflow.
    Useful for quick runs or when you already know good hyperparams.
    """
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    train_seqs, val_seqs, embedding_lookup, songs_df = load_training_data(
        num_sessions=num_sessions,
    )

    with mlflow.start_run(run_name="standalone_training"):
        mlflow.set_tag("run_type", "standalone")
        mlflow.log_param("catalog_size", len(songs_df))

        model, best_val_loss = train(
            train_seqs=train_seqs,
            val_seqs=val_seqs,
            embedding_lookup=embedding_lookup,
            hidden_dim=GRU_HIDDEN_DIM,
            num_layers=GRU_NUM_LAYERS,
            epochs=epochs,
            batch_size=batch_size,
            lr=lr,
            patience=patience,
            save_model=True,
        )

    return model


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Train SessionGRU")
    parser.add_argument(
        "--mode",
        choices=["standalone", "tune"],
        default="tune",
        help="'standalone' for single run, 'tune' for Optuna HPO"
    )
    parser.add_argument("--sessions", type=int, default=5000)
    parser.add_argument("--trials", type=int, default=20)
    args = parser.parse_args()

    if args.mode == "tune":
        tune_and_train(num_sessions=args.sessions, n_trials=args.trials)
    else:
        train_standalone(num_sessions=args.sessions)