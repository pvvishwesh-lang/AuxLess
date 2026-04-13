"""
Model Bias Detection for AuxLess Recommendation System.

Evaluates whether the recommendation engine produces fair results
across different data slices (genre, artist popularity, etc.).

Checks:
1. Genre representation bias - are certain genres over/under-represented
   in recommendations compared to the song catalog?
2. Popularity bias — does the model disproportionately recommend
   popular songs over niche content?
3. Per-genre recommendation quality — is CBF/GRU score consistent
   across genres or does it favor certain genres?

Used after training or during evaluation to ensure fairness.
Reports are logged to MLflow and saved as JSON to GCS.
"""

import logging
import numpy as np
import pandas as pd
from scipy.stats import entropy as scipy_entropy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
DOMINANCE_THRESHOLD    = 0.40   # genre > 40% of recommendations = bias flag
UNDERREP_THRESHOLD     = 0.02   # genre < 2% of recommendations = underrepresented
SCORE_DISPARITY_THRESHOLD = 0.15  # avg score difference > 0.15 between genres = flag


# ── Genre Representation Bias ─────────────────────────────────────────────────
def compute_genre_representation(
    recommendations_df: pd.DataFrame,
    catalog_df: pd.DataFrame,
) -> dict:
    """
    Compares genre distribution in recommendations vs the full catalog.
    Flags genres that are over-represented or under-represented
    relative to their catalog proportion.

    Args:
        recommendations_df: DataFrame with 'genre' column (recommended songs)
        catalog_df:         DataFrame with 'genre' column (full song catalog)

    Returns:
        dict with genre proportions, bias flags, and entropy scores
    """
    rec_counts   = recommendations_df["genre"].fillna("unknown").value_counts(normalize=True)
    cat_counts   = catalog_df["genre"].fillna("unknown").value_counts(normalize=True)

    # align indices
    all_genres   = sorted(set(rec_counts.index) | set(cat_counts.index))
    rec_props    = {g: round(rec_counts.get(g, 0.0), 4) for g in all_genres}
    cat_props    = {g: round(cat_counts.get(g, 0.0), 4) for g in all_genres}

    # compute deviation: how much recommendation distribution differs from catalog
    deviations = {}
    for genre in all_genres:
        rec_p = rec_props.get(genre, 0.0)
        cat_p = cat_props.get(genre, 0.0)
        deviations[genre] = round(rec_p - cat_p, 4)

    # identify biased genres
    over_represented  = [g for g, p in rec_props.items() if p > DOMINANCE_THRESHOLD]
    under_represented = [g for g, p in rec_props.items() if p < UNDERREP_THRESHOLD and cat_props.get(g, 0) >= UNDERREP_THRESHOLD]

    # entropy (higher = more diverse)
    rec_entropy = round(float(scipy_entropy(list(rec_props.values()))), 4) if rec_props else 0.0
    cat_entropy = round(float(scipy_entropy(list(cat_props.values()))), 4) if cat_props else 0.0

    report = {
        "recommendation_proportions": rec_props,
        "catalog_proportions":        cat_props,
        "deviations":                 deviations,
        "over_represented":           over_represented,
        "under_represented":          under_represented,
        "recommendation_entropy":     rec_entropy,
        "catalog_entropy":            cat_entropy,
        "entropy_ratio":              round(rec_entropy / max(cat_entropy, 1e-6), 4),
        "genre_bias_detected":        len(over_represented) > 0 or len(under_represented) > 0,
    }

    logger.info(
        f"Genre representation — "
        f"rec entropy: {rec_entropy}, catalog entropy: {cat_entropy}, "
        f"over-represented: {over_represented}, "
        f"under-represented: {under_represented}"
    )
    return report


# ── Popularity Bias ───────────────────────────────────────────────────────────
def compute_popularity_bias(
    recommendations_df: pd.DataFrame,
    catalog_df: pd.DataFrame,
) -> dict:
    """
    Checks if the recommender disproportionately favors popular songs.
    Compares average popularity_score of recommendations vs catalog.

    Args:
        recommendations_df: must have 'popularity_score' column
        catalog_df:         must have 'popularity_score' column

    Returns:
        dict with popularity stats and bias flag
    """
    if "popularity_score" not in recommendations_df.columns:
        logger.warning("popularity_score not in recommendations. Skipping popularity bias.")
        return {"popularity_bias_detected": False, "reason": "column_missing"}

    if "popularity_score" not in catalog_df.columns:
        logger.warning("popularity_score not in catalog. Skipping popularity bias.")
        return {"popularity_bias_detected": False, "reason": "column_missing"}

    rec_mean = float(recommendations_df["popularity_score"].mean())
    cat_mean = float(catalog_df["popularity_score"].mean())
    rec_med  = float(recommendations_df["popularity_score"].median())
    cat_med  = float(catalog_df["popularity_score"].median())

    # if recs avg popularity is >50% higher than catalog, flag it
    bias_ratio = rec_mean / max(cat_mean, 1e-6)
    popularity_bias = bias_ratio > 1.5

    report = {
        "rec_mean_popularity":     round(rec_mean, 4),
        "catalog_mean_popularity": round(cat_mean, 4),
        "rec_median_popularity":   round(rec_med, 4),
        "catalog_median_popularity": round(cat_med, 4),
        "popularity_bias_ratio":   round(bias_ratio, 4),
        "popularity_bias_detected": popularity_bias,
    }

    logger.info(
        f"Popularity bias — "
        f"rec mean: {rec_mean:.4f}, catalog mean: {cat_mean:.4f}, "
        f"ratio: {bias_ratio:.4f}, bias: {popularity_bias}"
    )
    return report


# ── Per-Genre Score Disparity ─────────────────────────────────────────────────
def compute_score_disparity(
    recommendations_df: pd.DataFrame,
    score_column: str = "final_score",
) -> dict:
    """
    Checks if recommendation scores are consistent across genres.
    If certain genres consistently get lower scores, the model
    may be biased against them.

    Args:
        recommendations_df: must have 'genre' and score_column
        score_column:       column name to analyze (default: 'final_score')

    Returns:
        dict with per-genre avg scores and disparity flag
    """
    if score_column not in recommendations_df.columns:
        logger.warning(f"{score_column} not in recommendations. Skipping score disparity.")
        return {"score_disparity_detected": False, "reason": "column_missing"}

    genre_scores = (
        recommendations_df
        .groupby(recommendations_df["genre"].fillna("unknown"))[score_column]
        .agg(["mean", "std", "count"])
        .round(4)
    )

    per_genre = {}
    for genre, row in genre_scores.iterrows():
        per_genre[genre] = {
            "mean_score": float(row["mean"]),
            "std_score":  float(row["std"]) if not pd.isna(row["std"]) else 0.0,
            "count":      int(row["count"]),
        }

    # check max disparity between any two genres
    means = [v["mean_score"] for v in per_genre.values() if v["count"] >= 3]
    if len(means) >= 2:
        max_disparity = max(means) - min(means)
        disparity_flag = max_disparity > SCORE_DISPARITY_THRESHOLD
    else:
        max_disparity = 0.0
        disparity_flag = False

    report = {
        "per_genre_scores":        per_genre,
        "max_score_disparity":     round(max_disparity, 4),
        "disparity_threshold":     SCORE_DISPARITY_THRESHOLD,
        "score_disparity_detected": disparity_flag,
    }

    logger.info(
        f"Score disparity — "
        f"max disparity: {max_disparity:.4f}, "
        f"threshold: {SCORE_DISPARITY_THRESHOLD}, "
        f"flag: {disparity_flag}"
    )
    return report


# ── Full Bias Report ──────────────────────────────────────────────────────────
def generate_bias_report(
    recommendations_df: pd.DataFrame,
    catalog_df: pd.DataFrame,
    score_column: str = "final_score",
) -> dict:
    """
    Runs all bias checks and produces a comprehensive report.

    Args:
        recommendations_df: recommended songs with genre, scores, popularity
        catalog_df:         full song catalog
        score_column:       which score column to analyze

    Returns:
        dict with genre_representation, popularity_bias,
        score_disparity, and overall_bias_detected flag
    """
    logger.info(
        f"Running model bias detection on "
        f"{len(recommendations_df)} recommendations "
        f"against {len(catalog_df)} catalog songs..."
    )

    genre_report      = compute_genre_representation(recommendations_df, catalog_df)
    popularity_report = compute_popularity_bias(recommendations_df, catalog_df)
    disparity_report  = compute_score_disparity(recommendations_df, score_column)

    overall_bias = (
        genre_report.get("genre_bias_detected", False) or
        popularity_report.get("popularity_bias_detected", False) or
        disparity_report.get("score_disparity_detected", False)
    )

    report = {
        "genre_representation": genre_report,
        "popularity_bias":      popularity_report,
        "score_disparity":      disparity_report,
        "overall_bias_detected": overall_bias,
        "recommendation_count": len(recommendations_df),
        "catalog_count":        len(catalog_df),
        "mitigation_suggestions": [],
    }

    # add mitigation suggestions based on findings
    if genre_report.get("genre_bias_detected"):
        if genre_report["over_represented"]:
            report["mitigation_suggestions"].append(
                f"Genres {genre_report['over_represented']} are over-represented. "
                f"Consider adding genre diversity constraints to the recommendation ranking."
            )
        if genre_report["under_represented"]:
            report["mitigation_suggestions"].append(
                f"Genres {genre_report['under_represented']} are under-represented. "
                f"Consider boosting scores for underrepresented genres or "
                f"upsampling these genres in training data."
            )

    if popularity_report.get("popularity_bias_detected"):
        report["mitigation_suggestions"].append(
            f"Popularity bias detected (ratio: {popularity_report['popularity_bias_ratio']}). "
            f"Consider adding a popularity penalty or diversity re-ranking step."
        )

    if disparity_report.get("score_disparity_detected"):
        report["mitigation_suggestions"].append(
            f"Score disparity across genres ({disparity_report['max_score_disparity']:.4f}). "
            f"Consider per-genre score normalization before final ranking."
        )

    if not overall_bias:
        report["mitigation_suggestions"].append(
            "No significant bias detected. Continue monitoring."
        )

    logger.info(
        f"Bias report complete. "
        f"Overall bias detected: {overall_bias}. "
        f"Suggestions: {len(report['mitigation_suggestions'])}"
    )
    return report

def evaluate(
    recommendations: list[dict],
    catalog_df: pd.DataFrame,
    score_column: str = "final_score",
) -> dict:
    """
    Adapter called from ml_trigger.py. Converts the recommendation engine's
    list-of-dicts into DataFrames and calls generate_bias_report().

    Args:
        recommendations: list of song dicts with keys:
                         video_id, title, artist, genre,
                         popularity_score, final_score,
                         cbf_score, cf_score, gru_score
        catalog_df:      the BigQuery catalog DataFrame already loaded in
                         Phase 2 of _run_ml_session() — pass it through,
                         do not re-fetch.
        score_column:    column to use for disparity check. Default: 'final_score'

    Returns:
        Same nested dict as generate_bias_report(). Never raises.
    """
    if not recommendations:
        logger.warning("evaluate() called with empty recommendations. Returning default.")
        return _empty_bias_report()

    try:
        recommendations_df = pd.DataFrame(recommendations)

        if "genre" not in recommendations_df.columns:
            recommendations_df["genre"] = "unknown"
        else:
            recommendations_df["genre"] = recommendations_df["genre"].fillna("unknown")

        if "popularity_score" not in recommendations_df.columns:
            recommendations_df["popularity_score"] = recommendations_df.get(
                "final_score", pd.Series(dtype=float)
            )

        if score_column not in recommendations_df.columns:
            logger.warning(
                f"score_column '{score_column}' not found. "
                f"Available: {list(recommendations_df.columns)}"
            )
            score_cols = [c for c in recommendations_df.columns if "score" in c.lower()]
            score_column = score_cols[0] if score_cols else "final_score"
            if score_column not in recommendations_df.columns:
                recommendations_df[score_column] = 0.0

        return generate_bias_report(recommendations_df, catalog_df, score_column)

    except Exception as exc:
        logger.error("evaluate() failed: %s", exc, exc_info=True)
        return _empty_bias_report()


def _empty_bias_report() -> dict:
    """Safe fallback with same shape as generate_bias_report()."""
    return {
        "genre_representation": {
            "recommendation_proportions": {},
            "catalog_proportions": {},
            "deviations": {},
            "over_represented": [],
            "under_represented": [],
            "recommendation_entropy": 0.0,
            "catalog_entropy": 0.0,
            "entropy_ratio": 1.0,
            "genre_bias_detected": False,
        },
        "popularity_bias": {
            "popularity_bias_detected": False,
            "reason": "evaluation_failed",
        },
        "score_disparity": {
            "per_genre_scores": {},
            "max_score_disparity": 0.0,
            "disparity_threshold": SCORE_DISPARITY_THRESHOLD,
            "score_disparity_detected": False,
        },
        "overall_bias_detected": False,
        "recommendation_count": 0,
        "catalog_count": 0,
        "mitigation_suggestions": ["Bias evaluation unavailable — check logs."],
    }

# ── Local testing ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # quick test with dummy data
    np.random.seed(42)
    genres = ["pop", "rock", "jazz", "hip-hop", "classical", "electronic"]

    catalog = pd.DataFrame({
        "genre": np.random.choice(genres, size=1000),
        "popularity_score": np.random.rand(1000),
    })

    # simulate biased recommendations (heavy pop)
    rec_genres = np.random.choice(
        genres, size=30,
        p=[0.5, 0.2, 0.1, 0.1, 0.05, 0.05]
    )
    recs = pd.DataFrame({
        "genre": rec_genres,
        "popularity_score": np.random.rand(30) * 0.8 + 0.2,
        "final_score": np.random.rand(30),
    })

    report = generate_bias_report(recs, catalog)

    import json
    print(json.dumps(report, indent=2))