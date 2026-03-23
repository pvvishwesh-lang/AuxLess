"""
Tests for GRU model architecture and inference.

Covers:
  - gru_model.py    → SessionGRU forward pass, output shape, L2 norm,
                      build_model
  - gru_inference.py → build_embedding_lookup, _build_sequence_tensor,
                       get_gru_scores (mocked model)

All tests run in CI (no GCP calls, no disk I/O).
Tests that require a trained model file are skipped in CI.
"""

import os
import numpy as np
import pandas as pd
import pytest
import torch


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_songs_df():
    np.random.seed(7)
    songs = []
    for i in range(20):
        songs.append({
            "video_id":    f"vid_{i}",
            "track_title": f"Song {i}",
            "artist_name": f"Artist {i % 4}",
            "genre":       ["pop", "rock", "jazz"][i % 3],
            "embedding":   np.random.rand(386).astype(np.float32),
        })
    return pd.DataFrame(songs)


@pytest.fixture
def embedding_lookup(sample_songs_df):
    return {
        row["video_id"]: np.array(row["embedding"], dtype=np.float32)
        for _, row in sample_songs_df.iterrows()
    }


@pytest.fixture
def sample_play_sequence():
    return [
        {"video_id": "vid_0", "play_order": 1, "liked_flag": 1},
        {"video_id": "vid_1", "play_order": 2, "liked_flag": 0},
        {"video_id": "vid_2", "play_order": 3, "liked_flag": 1},
        {"video_id": "vid_3", "play_order": 4, "liked_flag": 1},
    ]


@pytest.fixture
def gru_model():
    from ml.gru.gru_model import build_model
    return build_model(embedding_dim=386, hidden_dim=256, num_layers=2)


# ── SessionGRU architecture tests ─────────────────────────────────────────────

class TestSessionGRU:

    def test_output_shape(self, gru_model):
        dummy = torch.randn(4, 12, 387)   # batch=4, seq_len=12, input=387
        out   = gru_model(dummy)
        assert out.shape == (4, 386)

    def test_output_is_l2_normalized(self, gru_model):
        dummy = torch.randn(4, 12, 387)
        out   = gru_model(dummy)
        norms = torch.norm(out, dim=-1)
        assert torch.allclose(norms, torch.ones(4), atol=1e-5)

    def test_single_item_batch(self, gru_model):
        dummy = torch.randn(1, 5, 387)
        out   = gru_model(dummy)
        assert out.shape == (1, 386)

    def test_sequence_length_1(self, gru_model):
        dummy = torch.randn(2, 1, 387)
        out   = gru_model(dummy)
        assert out.shape == (2, 386)

    def test_sequence_length_15(self, gru_model):
        dummy = torch.randn(8, 15, 387)
        out   = gru_model(dummy)
        assert out.shape == (8, 386)

    def test_input_dim_is_387(self, gru_model):
        # 386 embedding + 1 liked_flag
        assert gru_model.input_dim == 387

    def test_embedding_dim_is_386(self, gru_model):
        assert gru_model.embedding_dim == 386

    def test_eval_mode_no_grad(self, gru_model):
        gru_model.eval()
        dummy = torch.randn(2, 5, 387)
        with torch.no_grad():
            out = gru_model(dummy)
        assert out.shape == (2, 386)

    def test_different_inputs_different_outputs(self, gru_model):
        gru_model.eval()
        x1 = torch.randn(1, 5, 387)
        x2 = torch.randn(1, 5, 387)
        with torch.no_grad():
            out1 = gru_model(x1)
            out2 = gru_model(x2)
        assert not torch.allclose(out1, out2)

    def test_parameter_count(self, gru_model):
        total = sum(p.numel() for p in gru_model.parameters())
        assert total > 0

    def test_no_nan_in_output(self, gru_model):
        dummy = torch.randn(4, 8, 387)
        out   = gru_model(dummy)
        assert not torch.isnan(out).any()

    def test_no_inf_in_output(self, gru_model):
        dummy = torch.randn(4, 8, 387)
        out   = gru_model(dummy)
        assert not torch.isinf(out).any()


class TestBuildModel:

    def test_returns_session_gru_instance(self):
        from ml.gru.gru_model import build_model, SessionGRU
        model = build_model()
        assert isinstance(model, SessionGRU)

    def test_custom_hidden_dim(self):
        from ml.gru.gru_model import build_model
        model = build_model(hidden_dim=128)
        assert model.hidden_dim == 128

    def test_custom_num_layers(self):
        from ml.gru.gru_model import build_model
        model = build_model(num_layers=3)
        assert model.num_layers == 3

    def test_default_embedding_dim(self):
        from ml.gru.gru_model import build_model
        model = build_model()
        assert model.embedding_dim == 386


# ── GRU inference tests ───────────────────────────────────────────────────────

class TestBuildEmbeddingLookup:

    def test_returns_dict(self, sample_songs_df):
        from ml.gru.gru_inference import build_embedding_lookup
        result = build_embedding_lookup(sample_songs_df)
        assert isinstance(result, dict)

    def test_all_video_ids_present(self, sample_songs_df):
        from ml.gru.gru_inference import build_embedding_lookup
        result   = build_embedding_lookup(sample_songs_df)
        expected = set(sample_songs_df["video_id"].tolist())
        assert set(result.keys()) == expected

    def test_embedding_shape(self, sample_songs_df):
        from ml.gru.gru_inference import build_embedding_lookup
        result = build_embedding_lookup(sample_songs_df)
        for vid, emb in result.items():
            assert emb.shape == (386,)

    def test_embedding_dtype(self, sample_songs_df):
        from ml.gru.gru_inference import build_embedding_lookup
        result = build_embedding_lookup(sample_songs_df)
        for vid, emb in result.items():
            assert emb.dtype == np.float32

    def test_empty_df_returns_empty_dict(self):
        from ml.gru.gru_inference import build_embedding_lookup
        empty_df = pd.DataFrame(columns=["video_id", "embedding"])
        result   = build_embedding_lookup(empty_df)
        assert result == {}


class TestBuildSequenceTensor:

    def test_returns_tensor(self, sample_play_sequence, embedding_lookup):
        from ml.gru.gru_inference import _build_sequence_tensor
        result = _build_sequence_tensor(sample_play_sequence, embedding_lookup)
        assert isinstance(result, torch.Tensor)

    def test_output_shape(self, sample_play_sequence, embedding_lookup):
        from ml.gru.gru_inference import _build_sequence_tensor
        result = _build_sequence_tensor(sample_play_sequence, embedding_lookup)
        # (1, MAX_SEQ_LEN, 387)
        assert result.shape[0] == 1
        assert result.shape[2] == 387

    def test_padded_to_max_seq_len(self, embedding_lookup):
        from ml.gru.gru_inference import _build_sequence_tensor, MAX_SEQ_LEN
        # only 2 events — should be padded to MAX_SEQ_LEN
        short_seq = [
            {"video_id": "vid_0", "play_order": 1, "liked_flag": 1},
            {"video_id": "vid_1", "play_order": 2, "liked_flag": 0},
        ]
        result = _build_sequence_tensor(short_seq, embedding_lookup)
        assert result.shape[1] == MAX_SEQ_LEN

    def test_padding_is_zeros(self, embedding_lookup):
        from ml.gru.gru_inference import _build_sequence_tensor, MAX_SEQ_LEN
        short_seq = [
            {"video_id": "vid_0", "play_order": 1, "liked_flag": 1},
        ]
        result = _build_sequence_tensor(short_seq, embedding_lookup)
        # first MAX_SEQ_LEN-1 rows should be zero-padded
        padding = result[0, :MAX_SEQ_LEN - 1, :]
        assert torch.all(padding == 0.0)

    def test_liked_flag_appended(self, embedding_lookup):
        from ml.gru.gru_inference import _build_sequence_tensor, MAX_SEQ_LEN
        seq = [{"video_id": "vid_0", "play_order": 1, "liked_flag": 1}]
        result = _build_sequence_tensor(seq, embedding_lookup)
        # last dim of last row should be the liked_flag (1.0)
        liked_flag_value = result[0, -1, -1].item()
        assert liked_flag_value == 1.0

    def test_unknown_video_id_skipped(self, embedding_lookup):
        from ml.gru.gru_inference import _build_sequence_tensor
        seq = [
            {"video_id": "nonexistent_vid", "play_order": 1, "liked_flag": 1},
            {"video_id": "vid_0",           "play_order": 2, "liked_flag": 0},
        ]
        result = _build_sequence_tensor(seq, embedding_lookup)
        # should still return a tensor (with only 1 valid event)
        assert result is not None

    def test_all_unknown_returns_none(self, embedding_lookup):
        from ml.gru.gru_inference import _build_sequence_tensor
        seq = [
            {"video_id": "ghost_1", "play_order": 1, "liked_flag": 1},
            {"video_id": "ghost_2", "play_order": 2, "liked_flag": 0},
        ]
        result = _build_sequence_tensor(seq, embedding_lookup)
        assert result is None

    def test_truncated_to_max_seq_len(self, embedding_lookup):
        from ml.gru.gru_inference import _build_sequence_tensor, MAX_SEQ_LEN
        # create a sequence longer than MAX_SEQ_LEN
        long_seq = [
            {"video_id": f"vid_{i % 20}", "play_order": i + 1, "liked_flag": i % 2}
            for i in range(MAX_SEQ_LEN + 5)
        ]
        result = _build_sequence_tensor(long_seq, embedding_lookup)
        assert result.shape[1] == MAX_SEQ_LEN


class TestGetGruScores:

    def test_returns_empty_df_below_threshold(
        self, gru_model, sample_play_sequence,
        embedding_lookup, sample_songs_df
    ):
        from ml.gru.gru_inference import get_gru_scores
        from ml.recommendation.config import GRU_ACTIVATION_THRESHOLD
        # use fewer songs than threshold
        short_seq = sample_play_sequence[:GRU_ACTIVATION_THRESHOLD - 1]
        result = get_gru_scores(
            model=gru_model,
            play_sequence=short_seq,
            embedding_lookup=embedding_lookup,
            songs_df=sample_songs_df,
            already_played_ids=set(),
        )
        assert result.empty

    def test_returns_dataframe_above_threshold(
        self, gru_model, embedding_lookup, sample_songs_df
    ):
        from ml.gru.gru_inference import get_gru_scores
        from ml.recommendation.config import GRU_ACTIVATION_THRESHOLD
        # build a sequence at least GRU_ACTIVATION_THRESHOLD long
        play_seq = [
            {"video_id": f"vid_{i}", "play_order": i + 1, "liked_flag": 1}
            for i in range(GRU_ACTIVATION_THRESHOLD)
        ]
        result = get_gru_scores(
            model=gru_model,
            play_sequence=play_seq,
            embedding_lookup=embedding_lookup,
            songs_df=sample_songs_df,
            already_played_ids=set(),
        )
        assert isinstance(result, pd.DataFrame)

    def test_returns_correct_columns(
        self, gru_model, embedding_lookup, sample_songs_df
    ):
        from ml.gru.gru_inference import get_gru_scores
        from ml.recommendation.config import GRU_ACTIVATION_THRESHOLD
        play_seq = [
            {"video_id": f"vid_{i}", "play_order": i + 1, "liked_flag": 1}
            for i in range(GRU_ACTIVATION_THRESHOLD)
        ]
        result = get_gru_scores(
            model=gru_model,
            play_sequence=play_seq,
            embedding_lookup=embedding_lookup,
            songs_df=sample_songs_df,
            already_played_ids=set(),
        )
        if not result.empty:
            expected = {"video_id", "track_title", "artist_name", "genre", "gru_score"}
            assert expected.issubset(set(result.columns))

    def test_excludes_already_played(
        self, gru_model, embedding_lookup, sample_songs_df
    ):
        from ml.gru.gru_inference import get_gru_scores
        from ml.recommendation.config import GRU_ACTIVATION_THRESHOLD
        play_seq = [
            {"video_id": f"vid_{i}", "play_order": i + 1, "liked_flag": 1}
            for i in range(GRU_ACTIVATION_THRESHOLD)
        ]
        already_played = {"vid_5", "vid_6", "vid_7"}
        result = get_gru_scores(
            model=gru_model,
            play_sequence=play_seq,
            embedding_lookup=embedding_lookup,
            songs_df=sample_songs_df,
            already_played_ids=already_played,
        )
        if not result.empty:
            assert len(set(result["video_id"]) & already_played) == 0

    def test_gru_scores_between_neg1_and_1(
        self, gru_model, embedding_lookup, sample_songs_df
    ):
        from ml.gru.gru_inference import get_gru_scores
        from ml.recommendation.config import GRU_ACTIVATION_THRESHOLD
        play_seq = [
            {"video_id": f"vid_{i}", "play_order": i + 1, "liked_flag": 1}
            for i in range(GRU_ACTIVATION_THRESHOLD)
        ]
        result = get_gru_scores(
            model=gru_model,
            play_sequence=play_seq,
            embedding_lookup=embedding_lookup,
            songs_df=sample_songs_df,
            already_played_ids=set(),
        )
        if not result.empty:
            assert result["gru_score"].between(-1.0, 1.0).all()

    def test_sorted_by_gru_score_descending(
        self, gru_model, embedding_lookup, sample_songs_df
    ):
        from ml.gru.gru_inference import get_gru_scores
        from ml.recommendation.config import GRU_ACTIVATION_THRESHOLD
        play_seq = [
            {"video_id": f"vid_{i}", "play_order": i + 1, "liked_flag": 1}
            for i in range(GRU_ACTIVATION_THRESHOLD)
        ]
        result = get_gru_scores(
            model=gru_model,
            play_sequence=play_seq,
            embedding_lookup=embedding_lookup,
            songs_df=sample_songs_df,
            already_played_ids=set(),
        )
        if not result.empty:
            scores = result["gru_score"].tolist()
            assert scores == sorted(scores, reverse=True)

    def test_empty_songs_df_returns_empty(
        self, gru_model, embedding_lookup
    ):
        from ml.gru.gru_inference import get_gru_scores
        from ml.recommendation.config import GRU_ACTIVATION_THRESHOLD
        play_seq = [
            {"video_id": f"vid_{i}", "play_order": i + 1, "liked_flag": 1}
            for i in range(GRU_ACTIVATION_THRESHOLD)
        ]
        empty_df = pd.DataFrame(
            columns=["video_id", "track_title", "artist_name", "genre", "embedding"]
        )
        result = get_gru_scores(
            model=gru_model,
            play_sequence=play_seq,
            embedding_lookup=embedding_lookup,
            songs_df=empty_df,
            already_played_ids=set(),
        )
        assert result.empty


# ── Live GCP tests (skipped in CI) ────────────────────────────────────────────

@pytest.mark.skipif(os.getenv("CI") == "true", reason="Skipped in CI — requires GCP")
def test_load_gru_model_live():
    from ml.gru.gru_inference import load_gru_model
    model = load_gru_model()
    assert model is not None


@pytest.mark.skipif(os.getenv("CI") == "true", reason="Skipped in CI — requires trained model")
def test_full_gru_inference_pipeline_live():
    from ml.gru.gru_inference import (
        load_gru_model, build_embedding_lookup, get_gru_scores
    )
    from ml.recommendation.bigquery_client import get_client, fetch_all_embeddings
    import os

    client   = get_client()
    songs_df = fetch_all_embeddings(client)
    lookup   = build_embedding_lookup(songs_df)
    model    = load_gru_model()

    play_seq = [
        {"video_id": songs_df["video_id"].iloc[i], "play_order": i + 1, "liked_flag": 1}
        for i in range(4)
    ]
    result = get_gru_scores(
        model=model,
        play_sequence=play_seq,
        embedding_lookup=lookup,
        songs_df=songs_df,
        already_played_ids=set(),
    )
    assert not result.empty
    assert "gru_score" in result.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
