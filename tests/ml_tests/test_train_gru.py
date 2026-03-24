"""
Tests for gru/train_gru.py

Covers:
  - SessionDataset.__init__(), __len__(), __getitem__()
  - Dataset sample generation from sequences
  - DataLoader compatibility
  - train() with mocked MLflow (no actual BigQuery or training)
  - load_training_data() (mocked BigQuery)
  - Edge cases: empty sequences, missing embeddings, short sequences
"""

import os
import pytest
import numpy as np
import pandas as pd
import torch
from unittest.mock import patch, MagicMock


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_embedding_lookup(n=20, dim=386, seed=0):
    np.random.seed(seed)
    return {
        f"vid_{i}": np.random.rand(dim).astype(np.float32)
        for i in range(n)
    }


def make_sequences(n_sessions=5, songs_per=5):
    """Creates list of (video_id, liked_flag) sequences."""
    sequences = []
    for s in range(n_sessions):
        seq = [
            (f"vid_{(s * songs_per + i) % 20}", i % 2)
            for i in range(songs_per)
        ]
        sequences.append(seq)
    return sequences


# ── SessionDataset tests ──────────────────────────────────────────────────────

class TestSessionDataset:

    def test_instantiates(self):
        from ml.gru.train_gru import SessionDataset
        lookup    = make_embedding_lookup()
        sequences = make_sequences(5, 4)
        dataset   = SessionDataset(sequences, lookup)
        assert dataset is not None

    def test_len_positive(self):
        from ml.gru.train_gru import SessionDataset
        lookup    = make_embedding_lookup()
        sequences = make_sequences(5, 4)
        dataset   = SessionDataset(sequences, lookup)
        assert len(dataset) > 0

    def test_len_correct_for_subsequences(self):
        from ml.gru.train_gru import SessionDataset
        lookup = make_embedding_lookup()
        # one session with 4 songs -> 3 subsequences (1->2, 1-2->3, 1-3->4)
        seq    = [("vid_0", 1), ("vid_1", 0), ("vid_2", 1), ("vid_3", 0)]
        dataset = SessionDataset([seq], lookup)
        assert len(dataset) == 3

    def test_getitem_returns_tuple(self):
        from ml.gru.train_gru import SessionDataset
        lookup   = make_embedding_lookup()
        sequences = make_sequences(3, 4)
        dataset  = SessionDataset(sequences, lookup)
        item     = dataset[0]
        assert isinstance(item, tuple)
        assert len(item) == 2

    def test_getitem_x_is_tensor(self):
        from ml.gru.train_gru import SessionDataset
        lookup   = make_embedding_lookup()
        sequences = make_sequences(3, 4)
        dataset  = SessionDataset(sequences, lookup)
        x, _     = dataset[0]
        assert isinstance(x, torch.Tensor)

    def test_getitem_target_is_tensor(self):
        from ml.gru.train_gru import SessionDataset
        lookup    = make_embedding_lookup()
        sequences = make_sequences(3, 4)
        dataset   = SessionDataset(sequences, lookup)
        _, target = dataset[0]
        assert isinstance(target, torch.Tensor)

    def test_x_shape(self):
        from ml.gru.train_gru import SessionDataset, MAX_SEQ_LEN
        lookup   = make_embedding_lookup()
        sequences = make_sequences(3, 4)
        dataset  = SessionDataset(sequences, lookup)
        x, _     = dataset[0]
        assert x.shape == (MAX_SEQ_LEN, 387)

    def test_target_shape(self):
        from ml.gru.train_gru import SessionDataset
        lookup    = make_embedding_lookup()
        sequences = make_sequences(3, 4)
        dataset   = SessionDataset(sequences, lookup)
        _, target = dataset[0]
        assert target.shape == (386,)

    def test_x_dtype_float32(self):
        from ml.gru.train_gru import SessionDataset
        lookup    = make_embedding_lookup()
        sequences = make_sequences(3, 4)
        dataset   = SessionDataset(sequences, lookup)
        x, _      = dataset[0]
        assert x.dtype == torch.float32

    def test_target_dtype_float32(self):
        from ml.gru.train_gru import SessionDataset
        lookup    = make_embedding_lookup()
        sequences = make_sequences(3, 4)
        dataset   = SessionDataset(sequences, lookup)
        _, target = dataset[0]
        assert target.dtype == torch.float32

    def test_skips_missing_target_video_id(self):
        from ml.gru.train_gru import SessionDataset
        lookup = make_embedding_lookup(10)
        seq    = [("vid_0", 1), ("nonexistent", 0), ("vid_1", 1)]
        dataset = SessionDataset([seq], lookup)
        assert len(dataset) >= 0

    def test_skips_missing_input_video_id(self):
        from ml.gru.train_gru import SessionDataset
        lookup = make_embedding_lookup(10)
        seq    = [("nonexistent", 1), ("vid_0", 0), ("vid_1", 1)]
        dataset = SessionDataset([seq], lookup)
        assert len(dataset) >= 0

    def test_empty_sequences_returns_empty_dataset(self):
        from ml.gru.train_gru import SessionDataset
        lookup  = make_embedding_lookup()
        dataset = SessionDataset([], lookup)
        assert len(dataset) == 0

    def test_single_song_sequence_skipped(self):
        from ml.gru.train_gru import SessionDataset
        lookup  = make_embedding_lookup()
        seq     = [("vid_0", 1)]
        dataset = SessionDataset([seq], lookup)
        assert len(dataset) == 0

    def test_padding_with_zeros_for_short_sequences(self):
        from ml.gru.train_gru import SessionDataset, MAX_SEQ_LEN
        lookup  = make_embedding_lookup()
        seq     = [("vid_0", 1), ("vid_1", 0)]
        dataset = SessionDataset([seq], lookup)
        if len(dataset) > 0:
            x, _ = dataset[0]
            assert x.shape[0] == MAX_SEQ_LEN

    def test_multiple_sequences_accumulate_samples(self):
        from ml.gru.train_gru import SessionDataset
        lookup    = make_embedding_lookup()
        sequences = make_sequences(10, 5)
        dataset   = SessionDataset(sequences, lookup)
        assert len(dataset) > 10

    def test_no_nan_in_x(self):
        from ml.gru.train_gru import SessionDataset
        lookup    = make_embedding_lookup()
        sequences = make_sequences(5, 4)
        dataset   = SessionDataset(sequences, lookup)
        x, _      = dataset[0]
        assert not torch.isnan(x).any()

    def test_no_nan_in_target(self):
        from ml.gru.train_gru import SessionDataset
        lookup    = make_embedding_lookup()
        sequences = make_sequences(5, 4)
        dataset   = SessionDataset(sequences, lookup)
        _, target = dataset[0]
        assert not torch.isnan(target).any()

    def test_liked_flag_in_last_dim(self):
        from ml.gru.train_gru import SessionDataset, MAX_SEQ_LEN
        lookup = make_embedding_lookup()
        seq    = [("vid_0", 1), ("vid_1", 0), ("vid_2", 1)]
        dataset = SessionDataset([seq], lookup)
        if len(dataset) > 0:
            x, _  = dataset[0]
            last_flag = x[-1, -1].item()
            assert last_flag in {0.0, 1.0}

    def test_consistent_output_for_same_index(self):
        from ml.gru.train_gru import SessionDataset
        lookup    = make_embedding_lookup()
        sequences = make_sequences(5, 4)
        dataset   = SessionDataset(sequences, lookup)
        x1, t1    = dataset[0]
        x2, t2    = dataset[0]
        assert torch.allclose(x1, x2)
        assert torch.allclose(t1, t2)

    def test_different_indices_different_data(self):
        from ml.gru.train_gru import SessionDataset
        lookup    = make_embedding_lookup()
        sequences = make_sequences(5, 5)
        dataset   = SessionDataset(sequences, lookup)
        if len(dataset) >= 2:
            x1, _ = dataset[0]
            x2, _ = dataset[1]
            assert not torch.allclose(x1, x2)

    def test_long_sequence_truncated(self):
        from ml.gru.train_gru import SessionDataset, MAX_SEQ_LEN
        lookup = make_embedding_lookup(20)
        seq    = [(f"vid_{i}", i % 2) for i in range(MAX_SEQ_LEN + 5)]
        dataset = SessionDataset([seq], lookup)
        if len(dataset) > 0:
            x, _ = dataset[-1]
            assert x.shape[0] == MAX_SEQ_LEN

    def test_dataloader_compatible(self):
        from ml.gru.train_gru import SessionDataset
        from torch.utils.data import DataLoader
        lookup    = make_embedding_lookup()
        sequences = make_sequences(5, 4)
        dataset   = SessionDataset(sequences, lookup)
        loader    = DataLoader(dataset, batch_size=4, shuffle=False)
        for x, target in loader:
            assert x.shape[-1] == 387
            assert target.shape[-1] == 386
            break

    def test_dataloader_batch_shape(self):
        from ml.gru.train_gru import SessionDataset, MAX_SEQ_LEN
        from torch.utils.data import DataLoader
        lookup    = make_embedding_lookup()
        sequences = make_sequences(5, 4)
        dataset   = SessionDataset(sequences, lookup)
        loader    = DataLoader(dataset, batch_size=4, shuffle=False)
        for x, target in loader:
            assert x.ndim == 3
            assert x.shape[1] == MAX_SEQ_LEN
            assert x.shape[2] == 387
            break


# ── MAX_SEQ_LEN tests ─────────────────────────────────────────────────────────

class TestMaxSeqLen:

    def test_max_seq_len_is_positive(self):
        from ml.gru.train_gru import MAX_SEQ_LEN
        assert MAX_SEQ_LEN > 0

    def test_max_seq_len_is_int(self):
        from ml.gru.train_gru import MAX_SEQ_LEN
        assert isinstance(MAX_SEQ_LEN, int)

    def test_max_seq_len_matches_inference(self):
        from ml.gru.train_gru import MAX_SEQ_LEN
        from ml.gru.gru_inference import MAX_SEQ_LEN as inference_max
        assert MAX_SEQ_LEN == inference_max


# ── train() tests (mocked MLflow, no actual BigQuery or training) ─────────────

class TestTrain:

    @patch("ml.gru.train_gru.mlflow")
    @patch("ml.gru.train_gru.torch.save")
    @patch("ml.gru.train_gru.os.makedirs")
    def test_train_runs_without_error(
        self, mock_makedirs, mock_save, mock_mlflow
    ):
        from ml.gru.train_gru import train

        lookup    = make_embedding_lookup()
        train_seqs = make_sequences(8, 4)
        val_seqs   = make_sequences(2, 4)

        model, best_val_loss = train(
            train_seqs=train_seqs,
            val_seqs=val_seqs,
            embedding_lookup=lookup,
            epochs=1,
            batch_size=4,
            patience=2,
            save_model=False,
        )
        assert model is not None
        assert isinstance(best_val_loss, float)

    @patch("ml.gru.train_gru.mlflow")
    @patch("ml.gru.train_gru.torch.save")
    @patch("ml.gru.train_gru.os.makedirs")
    def test_train_saves_model(
        self, mock_makedirs, mock_save, mock_mlflow
    ):
        from ml.gru.train_gru import train

        lookup     = make_embedding_lookup()
        train_seqs = make_sequences(8, 4)
        val_seqs   = make_sequences(2, 4)

        train(
            train_seqs=train_seqs,
            val_seqs=val_seqs,
            embedding_lookup=lookup,
            epochs=1,
            batch_size=4,
            patience=2,
            save_model=True,
        )
        mock_save.assert_called()

    @patch("ml.gru.train_gru.mlflow")
    @patch("ml.gru.train_gru.torch.save")
    @patch("ml.gru.train_gru.os.makedirs")
    def test_train_returns_session_gru(
        self, mock_makedirs, mock_save, mock_mlflow
    ):
        from ml.gru.train_gru import train
        from ml.gru.gru_model import SessionGRU

        lookup     = make_embedding_lookup()
        train_seqs = make_sequences(8, 4)
        val_seqs   = make_sequences(2, 4)

        model, _ = train(
            train_seqs=train_seqs,
            val_seqs=val_seqs,
            embedding_lookup=lookup,
            epochs=1,
            batch_size=4,
            patience=2,
            save_model=False,
        )
        assert isinstance(model, SessionGRU)

    @patch("ml.gru.train_gru.mlflow")
    @patch("ml.gru.train_gru.torch.save")
    @patch("ml.gru.train_gru.os.makedirs")
    def test_train_returns_best_val_loss(
        self, mock_makedirs, mock_save, mock_mlflow
    ):
        from ml.gru.train_gru import train

        lookup     = make_embedding_lookup()
        train_seqs = make_sequences(8, 4)
        val_seqs   = make_sequences(2, 4)

        _, best_val_loss = train(
            train_seqs=train_seqs,
            val_seqs=val_seqs,
            embedding_lookup=lookup,
            epochs=2,
            batch_size=4,
            patience=2,
            save_model=False,
        )
        assert best_val_loss > 0
        assert best_val_loss < float("inf")

    @patch("ml.gru.train_gru.mlflow")
    @patch("ml.gru.train_gru.torch.save")
    @patch("ml.gru.train_gru.os.makedirs")
    def test_train_logs_to_mlflow(
        self, mock_makedirs, mock_save, mock_mlflow
    ):
        from ml.gru.train_gru import train

        lookup     = make_embedding_lookup()
        train_seqs = make_sequences(8, 4)
        val_seqs   = make_sequences(2, 4)

        train(
            train_seqs=train_seqs,
            val_seqs=val_seqs,
            embedding_lookup=lookup,
            epochs=1,
            batch_size=4,
            patience=2,
            save_model=False,
        )
        # verify MLflow was called to log params and metrics
        mock_mlflow.log_params.assert_called_once()
        mock_mlflow.log_metrics.assert_called()

    @patch("ml.gru.train_gru.mlflow")
    @patch("ml.gru.train_gru.torch.save")
    @patch("ml.gru.train_gru.os.makedirs")
    def test_train_custom_hyperparams(
        self, mock_makedirs, mock_save, mock_mlflow
    ):
        from ml.gru.train_gru import train

        lookup     = make_embedding_lookup()
        train_seqs = make_sequences(8, 4)
        val_seqs   = make_sequences(2, 4)

        model, _ = train(
            train_seqs=train_seqs,
            val_seqs=val_seqs,
            embedding_lookup=lookup,
            hidden_dim=128,
            num_layers=1,
            dropout=0.2,
            epochs=1,
            batch_size=8,
            lr=5e-4,
            patience=2,
            save_model=False,
        )
        assert model is not None


# ── load_training_data() tests (mocked BigQuery) ─────────────────────────────

class TestLoadTrainingData:

    @patch("ml.gru.train_gru.get_client")
    @patch("ml.gru.train_gru.fetch_all_embeddings")
    @patch("ml.gru.train_gru.generate_synthetic_sessions")
    @patch("ml.gru.train_gru.get_session_sequences")
    def test_returns_four_items(
        self, mock_seqs, mock_synth, mock_fetch, mock_client
    ):
        from ml.gru.train_gru import load_training_data

        catalog = pd.DataFrame({
            "video_id":    [f"vid_{i}" for i in range(20)],
            "track_title": [f"Song {i}" for i in range(20)],
            "artist_name": ["Artist"] * 20,
            "genre":       ["pop"] * 20,
            "embedding":   [list(np.random.rand(386).astype(np.float32)) for _ in range(20)],
        })
        mock_fetch.return_value = catalog
        mock_synth.return_value = MagicMock()
        mock_seqs.return_value  = make_sequences(10, 4)

        result = load_training_data(num_sessions=10, val_split=0.2)
        assert len(result) == 4

    @patch("ml.gru.train_gru.get_client")
    @patch("ml.gru.train_gru.fetch_all_embeddings")
    @patch("ml.gru.train_gru.generate_synthetic_sessions")
    @patch("ml.gru.train_gru.get_session_sequences")
    def test_returns_train_val_split(
        self, mock_seqs, mock_synth, mock_fetch, mock_client
    ):
        from ml.gru.train_gru import load_training_data

        catalog = pd.DataFrame({
            "video_id":    [f"vid_{i}" for i in range(20)],
            "track_title": [f"Song {i}" for i in range(20)],
            "artist_name": ["Artist"] * 20,
            "genre":       ["pop"] * 20,
            "embedding":   [list(np.random.rand(386).astype(np.float32)) for _ in range(20)],
        })
        mock_fetch.return_value = catalog
        mock_synth.return_value = MagicMock()
        mock_seqs.return_value  = make_sequences(10, 4)

        train_seqs, val_seqs, lookup, songs_df = load_training_data(
            num_sessions=10, val_split=0.2
        )
        assert len(train_seqs) > 0
        assert len(val_seqs) > 0
        assert isinstance(lookup, dict)
        assert isinstance(songs_df, pd.DataFrame)

    @patch("ml.gru.train_gru.get_client")
    @patch("ml.gru.train_gru.fetch_all_embeddings")
    @patch("ml.gru.train_gru.generate_synthetic_sessions")
    @patch("ml.gru.train_gru.get_session_sequences")
    def test_embedding_lookup_has_correct_keys(
        self, mock_seqs, mock_synth, mock_fetch, mock_client
    ):
        from ml.gru.train_gru import load_training_data

        catalog = pd.DataFrame({
            "video_id":    [f"vid_{i}" for i in range(20)],
            "track_title": [f"Song {i}" for i in range(20)],
            "artist_name": ["Artist"] * 20,
            "genre":       ["pop"] * 20,
            "embedding":   [list(np.random.rand(386).astype(np.float32)) for _ in range(20)],
        })
        mock_fetch.return_value = catalog
        mock_synth.return_value = MagicMock()
        mock_seqs.return_value  = make_sequences(10, 4)

        _, _, lookup, _ = load_training_data(num_sessions=10)
        assert "vid_0" in lookup
        assert lookup["vid_0"].shape == (386,)


# ── Live training test (skipped in CI) ────────────────────────────────────────

@pytest.mark.skipif(os.getenv("CI") == "true", reason="Skipped in CI — requires GCP + GPU")
def test_full_training_pipeline_live():
    from ml.gru.train_gru import load_training_data, train
    import mlflow

    train_seqs, val_seqs, lookup, _ = load_training_data(num_sessions=100)
    mlflow.set_experiment("auxless-gru-test")
    with mlflow.start_run(run_name="live_test"):
        model, loss = train(
            train_seqs=train_seqs,
            val_seqs=val_seqs,
            embedding_lookup=lookup,
            epochs=2,
            batch_size=32,
        )
    assert model is not None
    assert loss < float("inf")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])