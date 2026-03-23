import os

# ── GCP ───────────────────────────────────────────────────────────────────────
PROJECT_ID  = os.environ.get("PROJECT_ID",         "music-recommender-490319")
DATABASE_ID = os.environ.get("FIRESTORE_DATABASE",  "auxless")
BUCKET      = os.environ.get("BUCKET",              "your-gcs-bucket-name")

# ── BigQuery — song catalog ───────────────────────────────────────────────────
DATASET_ID = "song_recommendations"
TABLE_ID   = "song_embeddings"
TABLE_REF  = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
# → music-recommender-490319.song_recommendations.song_embeddings

# ── BigQuery — user DB ────────────────────────────────────────────────────────
USERS_TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}.users"
# → music-recommender-490319.song_recommendations.users

# ── Recommendation queue ──────────────────────────────────────────────────────
TOP_N                     = 30   # songs per recommendation batch
RECOMMENDATION_BATCH_SIZE = 30   # same as TOP_N
REFRESH_THRESHOLD         = 10   # refresh queue every N songs played

# ── Hybrid weights (cold start, songs 1-3, before GRU activates) ──────────────
WEIGHT_CBF_COLD = 0.6
WEIGHT_CF_COLD  = 0.4

# ── Hybrid weights (warm, songs 6+, GRU at full weight) ──────────────────────
WEIGHT_CBF_WARM = 0.4
WEIGHT_CF_WARM  = 0.3
WEIGHT_GRU_WARM = 0.3

# ── GRU activation and ramp ───────────────────────────────────────────────────
GRU_ACTIVATION_THRESHOLD  = 3    # GRU activates after this many songs played
GRU_FULL_WEIGHT_THRESHOLD = 6    # GRU reaches full weight after this many songs

# GRU weight ramp between songs 4-6
# gradually increases GRU influence as sequence gets longer
# CBF and CF weights are normalized to sum to 1 at each step
GRU_WEIGHT_RAMP = {
    4: 0.20,   # song 4: GRU enters softly
    5: 0.25,   # song 5: GRU grows
    6: 0.30,   # song 6+: GRU at full weight
}

# ── Feedback scoring (must match streaming pipeline WEIGHTS) ──────────────────
SCORE_LIKE         = +2.0
SCORE_REPLAY       = +1.5
SCORE_AUTO_LIKED   = +1.5   # host skipped after 70% of song duration
SCORE_AUTO_DISLIKE = -1.0   # host skipped before 70% of song duration
SCORE_DISLIKE      = -2.0

# ── Skip threshold ────────────────────────────────────────────────────────────
SKIP_THRESHOLD_PCT = 0.70   # 70% of song duration = auto liked

# ── GRU model ─────────────────────────────────────────────────────────────────
GRU_EMBEDDING_DIM = 386     # 384 MiniLM + 2 numeric features
GRU_HIDDEN_DIM    = 256
GRU_NUM_LAYERS    = 2
GRU_MODEL_PATH    = os.environ.get("GRU_MODEL_PATH", "ml/gru/gru_model.pt")