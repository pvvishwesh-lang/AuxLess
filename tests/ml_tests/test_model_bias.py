"""
Tests for ml/evaluation/model_bias.py

Covers:
  - compute_genre_representation()
  - compute_popularity_bias()
  - compute_score_disparity()
  - generate_bias_report()
"""

import pytest
import numpy as np
import pandas as pd


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_catalog(n=100, seed=42):
    np.random.seed(seed)
    genres = ["pop", "rock", "jazz", "hip-hop", "classical"]
    return pd.DataFrame({
        "genre": [genres[i % len(genres)] for i in range(n)],
        "popularity_score": np.random.rand(n),
    })


def make_recommendations(n=30, genres=None, seed=42):
    np.random.seed(seed)
    if genres is None:
        genres = ["pop", "rock", "jazz", "hip-hop", "classical"]
    return pd.DataFrame({
        "genre": [genres[i % len(genres)] for i in range(n)],
        "popularity_score": np.random.rand(n),
        "final_score": np.random.rand(n),
    })


def make_biased_recommendations(n=30, seed=42):
    """Recommendations heavily biased toward pop."""
    np.random.seed(seed)
    genres = (["pop"] * 20) + (["rock"] * 5) + (["jazz"] * 3) + (["hip-hop"] * 2)
    return pd.DataFrame({
        "genre": genres[:n],
        "popularity_score": np.random.rand(n),
        "final_score": np.random.rand(n),
    })


# ── compute_genre_representation tests ────────────────────────────────────────

class TestComputeGenreRepresentation:

    def test_returns_dict(self):
        from ml.evaluation.model_bias import compute_genre_representation
        recs    = make_recommendations()
        catalog = make_catalog()
        result  = compute_genre_representation(recs, catalog)
        assert isinstance(result, dict)

    def test_has_recommendation_proportions(self):
        from ml.evaluation.model_bias import compute_genre_representation
        result = compute_genre_representation(make_recommendations(), make_catalog())
        assert "recommendation_proportions" in result

    def test_has_catalog_proportions(self):
        from ml.evaluation.model_bias import compute_genre_representation
        result = compute_genre_representation(make_recommendations(), make_catalog())
        assert "catalog_proportions" in result

    def test_has_deviations(self):
        from ml.evaluation.model_bias import compute_genre_representation
        result = compute_genre_representation(make_recommendations(), make_catalog())
        assert "deviations" in result

    def test_proportions_sum_to_one(self):
        from ml.evaluation.model_bias import compute_genre_representation
        result = compute_genre_representation(make_recommendations(), make_catalog())
        total  = sum(result["recommendation_proportions"].values())
        assert abs(total - 1.0) < 0.01

    def test_balanced_no_bias(self):
        from ml.evaluation.model_bias import compute_genre_representation
        recs    = make_recommendations(100)
        catalog = make_catalog(100)
        result  = compute_genre_representation(recs, catalog)
        assert result["genre_bias_detected"] is False

    def test_biased_detects_over_representation(self):
        from ml.evaluation.model_bias import compute_genre_representation
        recs    = make_biased_recommendations()
        catalog = make_catalog()
        result  = compute_genre_representation(recs, catalog)
        assert len(result["over_represented"]) > 0

    def test_entropy_is_positive(self):
        from ml.evaluation.model_bias import compute_genre_representation
        result = compute_genre_representation(make_recommendations(), make_catalog())
        assert result["recommendation_entropy"] > 0

    def test_entropy_ratio_close_to_one_for_balanced(self):
        from ml.evaluation.model_bias import compute_genre_representation
        recs    = make_recommendations(100)
        catalog = make_catalog(100)
        result  = compute_genre_representation(recs, catalog)
        assert 0.8 < result["entropy_ratio"] < 1.2

    def test_handles_unknown_genre(self):
        from ml.evaluation.model_bias import compute_genre_representation
        recs = pd.DataFrame({"genre": [None, "pop", "rock"]})
        catalog = make_catalog()
        result = compute_genre_representation(recs, catalog)
        assert "unknown" in result["recommendation_proportions"]


# ── compute_popularity_bias tests ─────────────────────────────────────────────

class TestComputePopularityBias:

    def test_returns_dict(self):
        from ml.evaluation.model_bias import compute_popularity_bias
        result = compute_popularity_bias(make_recommendations(), make_catalog())
        assert isinstance(result, dict)

    def test_no_bias_for_similar_popularity(self):
        from ml.evaluation.model_bias import compute_popularity_bias
        recs    = make_recommendations()
        catalog = make_catalog()
        result  = compute_popularity_bias(recs, catalog)
        assert result["popularity_bias_detected"] is False

    def test_detects_high_popularity_bias(self):
        from ml.evaluation.model_bias import compute_popularity_bias
        np.random.seed(42)
        recs = pd.DataFrame({"popularity_score": np.random.rand(30) * 0.3 + 0.7})
        catalog = pd.DataFrame({"popularity_score": np.random.rand(1000) * 0.3})
        result = compute_popularity_bias(recs, catalog)
        assert result["popularity_bias_detected"] is True

    def test_has_bias_ratio(self):
        from ml.evaluation.model_bias import compute_popularity_bias
        result = compute_popularity_bias(make_recommendations(), make_catalog())
        assert "popularity_bias_ratio" in result

    def test_missing_column_no_crash(self):
        from ml.evaluation.model_bias import compute_popularity_bias
        recs = pd.DataFrame({"genre": ["pop"]})
        catalog = make_catalog()
        result = compute_popularity_bias(recs, catalog)
        assert result["popularity_bias_detected"] is False

    def test_rec_mean_is_float(self):
        from ml.evaluation.model_bias import compute_popularity_bias
        result = compute_popularity_bias(make_recommendations(), make_catalog())
        assert isinstance(result["rec_mean_popularity"], float)


# ── compute_score_disparity tests ─────────────────────────────────────────────

class TestComputeScoreDisparity:

    def test_returns_dict(self):
        from ml.evaluation.model_bias import compute_score_disparity
        result = compute_score_disparity(make_recommendations())
        assert isinstance(result, dict)

    def test_has_per_genre_scores(self):
        from ml.evaluation.model_bias import compute_score_disparity
        result = compute_score_disparity(make_recommendations())
        assert "per_genre_scores" in result

    def test_no_disparity_for_uniform_scores(self):
        from ml.evaluation.model_bias import compute_score_disparity
        recs = pd.DataFrame({
            "genre": ["pop"] * 10 + ["rock"] * 10 + ["jazz"] * 10,
            "final_score": [0.5] * 30,
        })
        result = compute_score_disparity(recs)
        assert result["score_disparity_detected"] is False

    def test_detects_high_disparity(self):
        from ml.evaluation.model_bias import compute_score_disparity
        recs = pd.DataFrame({
            "genre": ["pop"] * 10 + ["rock"] * 10 + ["jazz"] * 10,
            "final_score": [0.9] * 10 + [0.5] * 10 + [0.2] * 10,
        })
        result = compute_score_disparity(recs)
        assert result["score_disparity_detected"] is True

    def test_missing_column_no_crash(self):
        from ml.evaluation.model_bias import compute_score_disparity
        recs = pd.DataFrame({"genre": ["pop"]})
        result = compute_score_disparity(recs)
        assert result["score_disparity_detected"] is False

    def test_max_disparity_is_float(self):
        from ml.evaluation.model_bias import compute_score_disparity
        result = compute_score_disparity(make_recommendations())
        assert isinstance(result["max_score_disparity"], float)

    def test_single_genre_no_disparity(self):
        from ml.evaluation.model_bias import compute_score_disparity
        recs = pd.DataFrame({
            "genre": ["pop"] * 10,
            "final_score": np.random.rand(10),
        })
        result = compute_score_disparity(recs)
        assert result["score_disparity_detected"] is False


# ── generate_bias_report tests ────────────────────────────────────────────────

class TestGenerateBiasReport:

    def test_returns_dict(self):
        from ml.evaluation.model_bias import generate_bias_report
        result = generate_bias_report(make_recommendations(), make_catalog())
        assert isinstance(result, dict)

    def test_has_all_sections(self):
        from ml.evaluation.model_bias import generate_bias_report
        result = generate_bias_report(make_recommendations(), make_catalog())
        assert "genre_representation" in result
        assert "popularity_bias" in result
        assert "score_disparity" in result
        assert "overall_bias_detected" in result

    def test_has_mitigation_suggestions(self):
        from ml.evaluation.model_bias import generate_bias_report
        result = generate_bias_report(make_recommendations(), make_catalog())
        assert "mitigation_suggestions" in result
        assert isinstance(result["mitigation_suggestions"], list)

    def test_no_bias_for_balanced_data(self):
        from ml.evaluation.model_bias import generate_bias_report
        recs    = make_recommendations(100)
        catalog = make_catalog(100)
        result  = generate_bias_report(recs, catalog)
        assert result["overall_bias_detected"] is False

    def test_detects_bias_for_skewed_data(self):
        from ml.evaluation.model_bias import generate_bias_report
        recs    = make_biased_recommendations()
        catalog = make_catalog()
        result  = generate_bias_report(recs, catalog)
        assert result["overall_bias_detected"] is True

    def test_suggestions_present_when_biased(self):
        from ml.evaluation.model_bias import generate_bias_report
        recs    = make_biased_recommendations()
        catalog = make_catalog()
        result  = generate_bias_report(recs, catalog)
        assert len(result["mitigation_suggestions"]) > 0

    def test_recommendation_count_correct(self):
        from ml.evaluation.model_bias import generate_bias_report
        recs    = make_recommendations(30)
        catalog = make_catalog(100)
        result  = generate_bias_report(recs, catalog)
        assert result["recommendation_count"] == 30

    def test_catalog_count_correct(self):
        from ml.evaluation.model_bias import generate_bias_report
        recs    = make_recommendations(30)
        catalog = make_catalog(100)
        result  = generate_bias_report(recs, catalog)
        assert result["catalog_count"] == 100

    def test_empty_recommendations(self):
        from ml.evaluation.model_bias import generate_bias_report
        recs = pd.DataFrame(columns=["genre", "popularity_score", "final_score"])
        catalog = make_catalog()
        result = generate_bias_report(recs, catalog)
        assert isinstance(result, dict)

    def test_single_recommendation(self):
        from ml.evaluation.model_bias import generate_bias_report
        recs = pd.DataFrame({
            "genre": ["pop"],
            "popularity_score": [0.5],
            "final_score": [0.8],
        })
        catalog = make_catalog()
        result = generate_bias_report(recs, catalog)
        assert isinstance(result, dict)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])