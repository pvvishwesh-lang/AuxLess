"""
GRU model architecture for session-based music recommendation.

Input:  sequence of (song_embedding + liked_flag) vectors
        shape: (batch, seq_len, 387)
               386 dims from song embedding + 1 dim for liked_flag

Output: predicted next song embedding
        shape: (batch, 386)

At inference: cosine similarity between predicted embedding
              and all songs in BigQuery catalog → ranked recommendations
"""

import torch
import torch.nn as nn
import torch.nn.functional as F


class SessionGRU(nn.Module):
    """
    GRU-based session model.

    Architecture:
    Input (387 dims = 386 embedding + 1 liked_flag)
        ↓
    Input projection (387 → hidden_dim)
        ↓
    GRU layers (hidden_dim, num_layers, dropout)
        ↓
    Final hidden state (hidden_dim)
        ↓
    FC projection (hidden_dim → embedding_dim)
        ↓
    L2 normalization
        ↓
    Predicted next song embedding (386 dims)
    """

    def __init__(
        self,
        embedding_dim: int   = 386,
        hidden_dim:    int   = 256,
        num_layers:    int   = 2,
        dropout:       float = 0.3,
    ):
        super().__init__()

        self.embedding_dim = embedding_dim
        self.hidden_dim    = hidden_dim
        self.num_layers    = num_layers
        self.input_dim     = embedding_dim + 1  # 386 + 1 liked_flag = 387

        # input projection: helps GRU learn better representations
        self.input_proj = nn.Sequential(
            nn.Linear(self.input_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
        )

        self.gru = nn.GRU(
            input_size=hidden_dim,
            hidden_size=hidden_dim,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )

        # output projection: hidden_dim → embedding_dim
        self.output_proj = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, embedding_dim),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """
        Args:
            x: (batch_size, seq_len, 387)

        Returns:
            predicted_embedding: (batch_size, 386) L2 normalized
        """
        # project input to hidden_dim
        # x shape: (batch, seq_len, 387) → (batch, seq_len, hidden_dim)
        projected = self.input_proj(x)

        # run through GRU
        # gru_out: (batch, seq_len, hidden_dim)
        gru_out, _ = self.gru(projected)

        # take last time step
        # shape: (batch, hidden_dim)
        last_hidden = gru_out[:, -1, :]

        # project to embedding space
        # shape: (batch, embedding_dim)
        predicted = self.output_proj(last_hidden)

        # L2 normalize for cosine similarity
        predicted = F.normalize(predicted, p=2, dim=-1)

        return predicted


def build_model(
    embedding_dim: int   = 386,
    hidden_dim:    int   = 256,
    num_layers:    int   = 2,
    dropout:       float = 0.3,
) -> SessionGRU:
    """
    Builds and returns a SessionGRU model instance.
    """
    model        = SessionGRU(
        embedding_dim=embedding_dim,
        hidden_dim=hidden_dim,
        num_layers=num_layers,
        dropout=dropout,
    )
    total_params = sum(p.numel() for p in model.parameters())
    print(f"SessionGRU — total parameters: {total_params:,}")
    return model


if __name__ == "__main__":
    # quick sanity check
    model = build_model()
    dummy = torch.randn(4, 12, 387)   # batch=4, seq_len=12, input_dim=387
    out   = model(dummy)
    print(f"Input shape:  {dummy.shape}")
    print(f"Output shape: {out.shape}")   # should be (4, 386)
    print(f"Output norms: {torch.norm(out, dim=-1)}")  # should all be ~1.0