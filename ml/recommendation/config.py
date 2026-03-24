import os

# ── GCP ───────────────────────────────────────────────────────────────────────
PROJECT_ID  = os.environ.get("PROJECT_ID",         "music-recommender-490319")
DATABASE_ID = os.environ.get("FIRESTORE_DATABASE",  "auxless")
BUCKET      = os.environ.get("BUCKET",              "youtube-pipeline-staging-bucket")

# ── BigQuery — song catalog ───────────────────────────────────────────────────
DATASET_ID = "song_recommendations"
TABLE_ID   = "song_embeddings"
TABLE_REF  = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# ── BigQuery — user DB ────────────────────────────────────────────────────────
USERS_TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.users"

# ── Recommendation queue ──────────────────────────────────────────────────────
TOP_N                     = 30
RECOMMENDATION_BATCH_SIZE = 30
REFRESH_THRESHOLD         = 10

# ── Hybrid weights (cold start, songs 1-3, before GRU activates) ──────────────
WEIGHT_CBF_COLD = 0.6
WEIGHT_CF_COLD  = 0.4

# ── Hybrid weights (warm, songs 6+, GRU at full weight) ──────────────────────
WEIGHT_CBF_WARM = 0.4
WEIGHT_CF_WARM  = 0.3
WEIGHT_GRU_WARM = 0.3

# ── GRU activation and ramp ───────────────────────────────────────────────────
GRU_ACTIVATION_THRESHOLD  = 3
GRU_FULL_WEIGHT_THRESHOLD = 6

GRU_WEIGHT_RAMP = {
    4: 0.20,
    5: 0.25,
    6: 0.30,
}

# ── Collaborative Filtering ──────────────────────────────────────────────────
CF_MIN_COOCCURRENCE = 2   # minimum co-occurrence count to trust as signal

# ── Feedback scoring (must match streaming pipeline WEIGHTS) ──────────────────
SCORE_LIKE         = +2.0
SCORE_REPLAY       = +1.5
SCORE_AUTO_LIKED   = +1.5
SCORE_AUTO_DISLIKE = -1.0
SCORE_DISLIKE      = -2.0

# ── Skip threshold ────────────────────────────────────────────────────────────
SKIP_THRESHOLD_PCT = 0.70

# ── GRU model ─────────────────────────────────────────────────────────────────
GRU_EMBEDDING_DIM = 386
GRU_HIDDEN_DIM    = 256
GRU_NUM_LAYERS    = 2
GRU_MODEL_PATH    = os.environ.get("GRU_MODEL_PATH", "ml/gru/gru_model.pt")