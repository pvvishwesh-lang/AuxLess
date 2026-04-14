"""
Tests for ml/evaluation/model_bias.py

Covers:
  - compute_genre_representation()
  - compute_popularity_bias()
  - compute_score_disparity()
  - generate_bias_report()
  - evaluate()              ← new: adapter called from ml_trigger.py
  - _empty_bias_report()    ← new: safe fallback shape validation
"""

import pytest
import numpy as np
import pandas as pd


# ── Shared constants (mirror model_bias.py thresholds) ───────────────────────
# These are defined here so tests are self-documenting about what they're
# probing. They must stay in sync with the thresholds in model_bias.py.

DOMINANCE_THRESHOLD       = 0.40
UNDERREP_THRESHOLD        = 0.02
SCORE_DISPARITY_THRESHOLD = 0.15


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
    """Recommendations heavily biased toward pop (>40%)."""
    np.random.seed(seed)
    genres = (["pop"] * 20) + (["rock"] * 5) + (["jazz"] * 3) + (["hip-hop"] * 2)
    return pd.DataFrame({
        "genre": genres[:n],
        "popularity_score": np.random.rand(n),
        "final_score": np.random.rand(n),
    })


def make_rec_list(n=30, seed=42, biased=False, uniform_scores=False):
    """
    Returns a list of dicts — the format produced by the recommendation engine
    and consumed by evaluate(). Mirrors what ml_trigger.py passes in.

    uniform_scores=True: all final_scores are 0.5 ± tiny noise so
    per-genre score means differ by < 0.01 — well below the 0.15
    score_disparity threshold. Use this when the test is checking
    genre/popularity bias only and random scores would accidentally
    trigger score_disparity_detected on otherwise balanced data.
    """
    np.random.seed(seed)
    genres = ["pop", "rock", "jazz", "hip-hop", "classical"]
    if biased:
        genre_pool = (["pop"] * 20) + (["rock"] * 5) + (["jazz"] * 3) + (["hip-hop"] * 2)
    else:
        genre_pool = genres * (n // len(genres) + 1)

    scores = (
        [round(0.5 + (i % 5) * 0.001, 4) for i in range(n)]
        if uniform_scores
        else [round(float(np.random.rand()), 4) for _ in range(n)]
    )

    return [
        {
            "video_id":        f"vid_{i}",
            "title":           f"Song {i}",
            "artist":          f"Artist {i}",
            "genre":           genre_pool[i],
            "popularity_score": round(float(np.random.rand()), 4),
            "cbf_score":       round(float(np.random.rand()), 4),
            "cf_score":        round(float(np.random.rand()), 4),
            "gru_score":       round(float(np.random.rand()), 4),
            "final_score":     scores[i],
        }
        for i in range(n)
    ]


# ── Expected output shape for generate_bias_report / evaluate ────────────────
# Used in multiple test classes to validate structural completeness.

EXPECTED_TOP_LEVEL_KEYS = {
    "genre_representation",
    "popularity_bias",
    "score_disparity",
    "overall_bias_detected",
    "recommendation_count",
    "catalog_count",
    "mitigation_suggestions",
}

EXPECTED_GENRE_REP_KEYS = {
    "recommendation_proportions",
    "catalog_proportions",
    "deviations",
    "over_represented",
    "under_represented",
    "recommendation_entropy",
    "catalog_entropy",
    "entropy_ratio",
    "genre_bias_detected",
}

EXPECTED_POP_BIAS_KEYS = {
    "rec_mean_popularity",
    "catalog_mean_popularity",
    "rec_median_popularity",
    "catalog_median_popularity",
    "popularity_bias_ratio",
    "popularity_bias_detected",
}

EXPECTED_SCORE_DISP_KEYS = {
    "per_genre_scores",
    "max_score_disparity",
    "disparity_threshold",
    "score_disparity_detected",
}


# ── compute_genre_representation tests ───────────────────────────────────────

class TestComputeGenreRepresentation:

    def test_returns_dict(self):
        from ml.evaluation.model_bias import compute_genre_representation
        result = compute_genre_representation(make_recommendations(), make_catalog())
        assert isinstance(result, dict)

    def test_has_all_keys(self):
        from ml.evaluation.model_bias import compute_genre_representation
        result = compute_genre_representation(make_recommendations(), make_catalog())
        assert EXPECTED_GENRE_REP_KEYS.issubset(result.keys())

    def test_proportions_sum_to_one(self):
        from ml.evaluation.model_bias import compute_genre_representation
        result = compute_genre_representation(make_recommendations(), make_catalog())
        total = sum(result["recommendation_proportions"].values())
        assert abs(total - 1.0) < 0.01

    def test_catalog_proportions_sum_to_one(self):
        from ml.evaluation.model_bias import compute_genre_representation
        result = compute_genre_representation(make_recommendations(), make_catalog())
        total = sum(result["catalog_proportions"].values())
        assert abs(total - 1.0) < 0.01

    def test_balanced_no_bias(self):
        from ml.evaluation.model_bias import compute_genre_representation
        result = compute_genre_representation(make_recommendations(100), make_catalog(100))
        assert result["genre_bias_detected"] is False

    def test_biased_detects_over_representation(self):
        from ml.evaluation.model_bias import compute_genre_representation
        result = compute_genre_representation(make_biased_recommendations(), make_catalog())
        assert len(result["over_represented"]) > 0

    def test_over_represented_exceeds_threshold(self):
        from ml.evaluation.model_bias import compute_genre_representation
        result = compute_genre_representation(make_biased_recommendations(), make_catalog())
        props = result["recommendation_proportions"]
        for genre in result["over_represented"]:
            assert props[genre] > DOMINANCE_THRESHOLD

    def test_entropy_is_positive(self):
        from ml.evaluation.model_bias import compute_genre_representation
        result = compute_genre_representation(make_recommendations(), make_catalog())
        assert result["recommendation_entropy"] > 0

    def test_entropy_ratio_close_to_one_for_balanced(self):
        from ml.evaluation.model_bias import compute_genre_representation
        result = compute_genre_representation(make_recommendations(100), make_catalog(100))
        assert 0.8 < result["entropy_ratio"] < 1.2

    def test_biased_entropy_ratio_below_one(self):
        # biased recs have lower entropy than the balanced catalog
        from ml.evaluation.model_bias import compute_genre_representation
        result = compute_genre_representation(make_biased_recommendations(), make_catalog(200))
        assert result["entropy_ratio"] < 1.0

    def test_deviations_keys_match_all_genres(self):
        from ml.evaluation.model_bias import compute_genre_representation
        result = compute_genre_representation(make_recommendations(), make_catalog())
        rec_genres = set(result["recommendation_proportions"].keys())
        cat_genres = set(result["catalog_proportions"].keys())
        assert set(result["deviations"].keys()) == rec_genres | cat_genres

    def test_handles_null_genre(self):
        from ml.evaluation.model_bias import compute_genre_representation
        recs = pd.DataFrame({"genre": [None, "pop", "rock"]})
        result = compute_genre_representation(recs, make_catalog())
        assert "unknown" in result["recommendation_proportions"]

    def test_genre_bias_detected_is_bool(self):
        from ml.evaluation.model_bias import compute_genre_representation
        result = compute_genre_representation(make_recommendations(), make_catalog())
        assert isinstance(result["genre_bias_detected"], bool)


# ── compute_popularity_bias tests ────────────────────────────────────────────

class TestComputePopularityBias:

    def test_returns_dict(self):
        from ml.evaluation.model_bias import compute_popularity_bias
        result = compute_popularity_bias(make_recommendations(), make_catalog())
        assert isinstance(result, dict)

    def test_has_all_keys_when_column_present(self):
        from ml.evaluation.model_bias import compute_popularity_bias
        result = compute_popularity_bias(make_recommendations(), make_catalog())
        assert EXPECTED_POP_BIAS_KEYS.issubset(result.keys())

    def test_no_bias_for_similar_popularity(self):
        from ml.evaluation.model_bias import compute_popularity_bias
        result = compute_popularity_bias(make_recommendations(), make_catalog())
        assert result["popularity_bias_detected"] is False

    def test_detects_high_popularity_bias(self):
        from ml.evaluation.model_bias import compute_popularity_bias
        np.random.seed(42)
        # recs skewed high, catalog skewed low → ratio well above 1.5
        recs = pd.DataFrame({"popularity_score": np.random.rand(30) * 0.3 + 0.7})
        catalog = pd.DataFrame({"popularity_score": np.random.rand(1000) * 0.3})
        result = compute_popularity_bias(recs, catalog)
        assert result["popularity_bias_detected"] is True

    def test_bias_ratio_reflects_actual_means(self):
        from ml.evaluation.model_bias import compute_popularity_bias
        recs = pd.DataFrame({"popularity_score": [0.9] * 10})
        catalog = pd.DataFrame({"popularity_score": [0.3] * 100})
        result = compute_popularity_bias(recs, catalog)
        assert abs(result["popularity_bias_ratio"] - (0.9 / 0.3)) < 0.05

    def test_missing_rec_column_no_crash(self):
        from ml.evaluation.model_bias import compute_popularity_bias
        recs = pd.DataFrame({"genre": ["pop"]})
        result = compute_popularity_bias(recs, make_catalog())
        assert result["popularity_bias_detected"] is False
        assert "reason" in result

    def test_missing_catalog_column_no_crash(self):
        from ml.evaluation.model_bias import compute_popularity_bias
        catalog = pd.DataFrame({"genre": ["pop"] * 50})
        result = compute_popularity_bias(make_recommendations(), catalog)
        assert result["popularity_bias_detected"] is False
        assert "reason" in result

    def test_rec_mean_is_float(self):
        from ml.evaluation.model_bias import compute_popularity_bias
        result = compute_popularity_bias(make_recommendations(), make_catalog())
        assert isinstance(result["rec_mean_popularity"], float)

    def test_popularity_bias_detected_is_bool(self):
        from ml.evaluation.model_bias import compute_popularity_bias
        result = compute_popularity_bias(make_recommendations(), make_catalog())
        assert isinstance(result["popularity_bias_detected"], bool)


# ── compute_score_disparity tests ────────────────────────────────────────────

class TestComputeScoreDisparity:

    def test_returns_dict(self):
        from ml.evaluation.model_bias import compute_score_disparity
        result = compute_score_disparity(make_recommendations())
        assert isinstance(result, dict)

    def test_has_all_keys(self):
        from ml.evaluation.model_bias import compute_score_disparity
        result = compute_score_disparity(make_recommendations())
        assert EXPECTED_SCORE_DISP_KEYS.issubset(result.keys())

    def test_no_disparity_for_uniform_scores(self):
        from ml.evaluation.model_bias import compute_score_disparity
        recs = pd.DataFrame({
            "genre":       ["pop"] * 10 + ["rock"] * 10 + ["jazz"] * 10,
            "final_score": [0.5] * 30,
        })
        result = compute_score_disparity(recs)
        assert result["score_disparity_detected"] is False

    def test_detects_high_disparity(self):
        from ml.evaluation.model_bias import compute_score_disparity
        recs = pd.DataFrame({
            "genre":       ["pop"] * 10 + ["rock"] * 10 + ["jazz"] * 10,
            "final_score": [0.9] * 10  + [0.5] * 10  + [0.2] * 10,
        })
        result = compute_score_disparity(recs)
        assert result["score_disparity_detected"] is True

    def test_max_disparity_value_is_correct(self):
        from ml.evaluation.model_bias import compute_score_disparity
        recs = pd.DataFrame({
            "genre":       ["pop"] * 10 + ["rock"] * 10,
            "final_score": [0.9] * 10  + [0.5] * 10,
        })
        result = compute_score_disparity(recs)
        assert abs(result["max_score_disparity"] - 0.4) < 0.01

    def test_per_genre_scores_has_mean(self):
        from ml.evaluation.model_bias import compute_score_disparity
        result = compute_score_disparity(make_recommendations())
        for genre, stats in result["per_genre_scores"].items():
            assert "mean_score" in stats
            assert "std_score" in stats
            assert "count" in stats

    def test_single_genre_no_disparity(self):
        from ml.evaluation.model_bias import compute_score_disparity
        recs = pd.DataFrame({
            "genre":       ["pop"] * 10,
            "final_score": np.random.rand(10),
        })
        result = compute_score_disparity(recs)
        assert result["score_disparity_detected"] is False

    def test_missing_column_no_crash(self):
        from ml.evaluation.model_bias import compute_score_disparity
        recs = pd.DataFrame({"genre": ["pop"]})
        result = compute_score_disparity(recs)
        assert result["score_disparity_detected"] is False
        assert "reason" in result

    def test_disparity_threshold_matches_constant(self):
        from ml.evaluation.model_bias import compute_score_disparity
        result = compute_score_disparity(make_recommendations())
        assert result["disparity_threshold"] == SCORE_DISPARITY_THRESHOLD

    def test_max_disparity_is_float(self):
        from ml.evaluation.model_bias import compute_score_disparity
        result = compute_score_disparity(make_recommendations())
        assert isinstance(result["max_score_disparity"], float)

    def test_genres_with_fewer_than_3_songs_excluded_from_disparity(self):
        # Genres with count < 3 are excluded from disparity calculation.
        # Only two valid genres, but one has fewer than 3 songs → no disparity flag.
        from ml.evaluation.model_bias import compute_score_disparity
        recs = pd.DataFrame({
            "genre":       ["pop"] * 10 + ["rare"] * 2,
            "final_score": [0.9] * 10  + [0.1] * 2,
        })
        result = compute_score_disparity(recs)
        # Only "pop" has >=3 songs, so means list has 1 entry → no disparity
        assert result["score_disparity_detected"] is False


# ── generate_bias_report tests ────────────────────────────────────────────────

class TestGenerateBiasReport:

    def test_returns_dict(self):
        from ml.evaluation.model_bias import generate_bias_report
        result = generate_bias_report(make_recommendations(), make_catalog())
        assert isinstance(result, dict)

    def test_has_all_top_level_keys(self):
        from ml.evaluation.model_bias import generate_bias_report
        result = generate_bias_report(make_recommendations(), make_catalog())
        assert EXPECTED_TOP_LEVEL_KEYS.issubset(result.keys())

    def test_genre_representation_has_all_keys(self):
        from ml.evaluation.model_bias import generate_bias_report
        result = generate_bias_report(make_recommendations(), make_catalog())
        assert EXPECTED_GENRE_REP_KEYS.issubset(result["genre_representation"].keys())

    def test_popularity_bias_has_all_keys(self):
        from ml.evaluation.model_bias import generate_bias_report
        result = generate_bias_report(make_recommendations(), make_catalog())
        assert EXPECTED_POP_BIAS_KEYS.issubset(result["popularity_bias"].keys())

    def test_score_disparity_has_all_keys(self):
        from ml.evaluation.model_bias import generate_bias_report
        result = generate_bias_report(make_recommendations(), make_catalog())
        assert EXPECTED_SCORE_DISP_KEYS.issubset(result["score_disparity"].keys())

    def test_mitigation_suggestions_is_list(self):
        from ml.evaluation.model_bias import generate_bias_report
        result = generate_bias_report(make_recommendations(), make_catalog())
        assert isinstance(result["mitigation_suggestions"], list)

    def test_no_bias_for_balanced_data(self):
        from ml.evaluation.model_bias import generate_bias_report
        result = generate_bias_report(make_recommendations(100), make_catalog(100))
        assert result["overall_bias_detected"] is False

    def test_detects_bias_for_skewed_data(self):
        from ml.evaluation.model_bias import generate_bias_report
        result = generate_bias_report(make_biased_recommendations(), make_catalog())
        assert result["overall_bias_detected"] is True

    def test_suggestions_present_when_biased(self):
        from ml.evaluation.model_bias import generate_bias_report
        result = generate_bias_report(make_biased_recommendations(), make_catalog())
        assert len(result["mitigation_suggestions"]) > 0

    def test_no_bias_suggestion_when_clean(self):
        from ml.evaluation.model_bias import generate_bias_report
        result = generate_bias_report(make_recommendations(100), make_catalog(100))
        # should have the "no bias" confirmation message
        assert any("No significant bias" in s for s in result["mitigation_suggestions"])

    def test_recommendation_count_correct(self):
        from ml.evaluation.model_bias import generate_bias_report
        result = generate_bias_report(make_recommendations(30), make_catalog(100))
        assert result["recommendation_count"] == 30

    def test_catalog_count_correct(self):
        from ml.evaluation.model_bias import generate_bias_report
        result = generate_bias_report(make_recommendations(30), make_catalog(100))
        assert result["catalog_count"] == 100

    def test_overall_bias_detected_is_bool(self):
        from ml.evaluation.model_bias import generate_bias_report
        result = generate_bias_report(make_recommendations(), make_catalog())
        assert isinstance(result["overall_bias_detected"], bool)

    def test_empty_recommendations_no_crash(self):
        from ml.evaluation.model_bias import generate_bias_report
        recs = pd.DataFrame(columns=["genre", "popularity_score", "final_score"])
        result = generate_bias_report(recs, make_catalog())
        assert isinstance(result, dict)

    def test_single_recommendation_no_crash(self):
        from ml.evaluation.model_bias import generate_bias_report
        recs = pd.DataFrame({
            "genre": ["pop"], "popularity_score": [0.5], "final_score": [0.8]
        })
        result = generate_bias_report(recs, make_catalog())
        assert isinstance(result, dict)

    def test_overall_bias_is_union_of_sub_checks(self):
        # overall_bias_detected must be True if ANY sub-check fires
        from ml.evaluation.model_bias import generate_bias_report
        result = generate_bias_report(make_biased_recommendations(), make_catalog())
        any_sub = (
            result["genre_representation"]["genre_bias_detected"] or
            result["popularity_bias"]["popularity_bias_detected"] or
            result["score_disparity"]["score_disparity_detected"]
        )
        assert result["overall_bias_detected"] == any_sub


# ── evaluate() tests ──────────────────────────────────────────────────────────

class TestEvaluate:
    """
    Tests for the evaluate() adapter added to model_bias.py.
    This is the function called from ml_trigger.py.
    It accepts list[dict] (recommendation engine output) + catalog DataFrame.
    """

    def test_returns_dict(self):
        from ml.evaluation.model_bias import evaluate
        result = evaluate(make_rec_list(), make_catalog())
        assert isinstance(result, dict)

    def test_has_all_top_level_keys(self):
        from ml.evaluation.model_bias import evaluate
        result = evaluate(make_rec_list(), make_catalog())
        assert EXPECTED_TOP_LEVEL_KEYS.issubset(result.keys())

    def test_genre_representation_sub_dict_complete(self):
        from ml.evaluation.model_bias import evaluate
        result = evaluate(make_rec_list(), make_catalog())
        assert EXPECTED_GENRE_REP_KEYS.issubset(result["genre_representation"].keys())

    def test_popularity_bias_sub_dict_complete(self):
        from ml.evaluation.model_bias import evaluate
        result = evaluate(make_rec_list(), make_catalog())
        assert EXPECTED_POP_BIAS_KEYS.issubset(result["popularity_bias"].keys())

    def test_score_disparity_sub_dict_complete(self):
        from ml.evaluation.model_bias import evaluate
        result = evaluate(make_rec_list(), make_catalog())
        assert EXPECTED_SCORE_DISP_KEYS.issubset(result["score_disparity"].keys())

    def test_detects_bias_for_skewed_recs(self):
        from ml.evaluation.model_bias import evaluate
        result = evaluate(make_rec_list(biased=True), make_catalog())
        assert result["overall_bias_detected"] is True

    def test_no_bias_for_balanced_recs(self):
        from ml.evaluation.model_bias import evaluate
        # uniform_scores=True keeps per-genre score means within 0.004 of each
        # other, well below the 0.15 score_disparity threshold. Random scores
        # would produce disparity > 0.15 even with perfectly balanced genres.
        result = evaluate(make_rec_list(n=100, uniform_scores=True), make_catalog(n=500))
        assert result["overall_bias_detected"] is False

    def test_empty_list_returns_safe_default(self):
        from ml.evaluation.model_bias import evaluate
        result = evaluate([], make_catalog())
        assert isinstance(result, dict)
        assert EXPECTED_TOP_LEVEL_KEYS.issubset(result.keys())

    def test_missing_genre_key_no_crash(self):
        from ml.evaluation.model_bias import evaluate
        recs = [
            {"video_id": "v1", "popularity_score": 0.5, "final_score": 0.7}
        ]
        result = evaluate(recs, make_catalog())
        assert isinstance(result, dict)

    def test_missing_popularity_score_key_no_crash(self):
        from ml.evaluation.model_bias import evaluate
        recs = [
            {"video_id": "v1", "genre": "pop", "final_score": 0.7}
        ]
        result = evaluate(recs, make_catalog())
        assert isinstance(result, dict)

    def test_missing_final_score_key_no_crash(self):
        from ml.evaluation.model_bias import evaluate
        recs = [
            {"video_id": "v1", "genre": "pop", "popularity_score": 0.5}
        ]
        result = evaluate(recs, make_catalog())
        assert isinstance(result, dict)

    def test_custom_score_column_used(self):
        from ml.evaluation.model_bias import evaluate
        recs = make_rec_list()
        # rename final_score to cbf_score as the column under test
        for r in recs:
            r["cbf_score"] = r.pop("final_score")
        result = evaluate(recs, make_catalog(), score_column="cbf_score")
        assert isinstance(result, dict)

    def test_recommendation_count_matches_input_length(self):
        from ml.evaluation.model_bias import evaluate
        recs = make_rec_list(n=20)
        result = evaluate(recs, make_catalog())
        assert result["recommendation_count"] == 20

    def test_catalog_count_matches_catalog_df(self):
        from ml.evaluation.model_bias import evaluate
        catalog = make_catalog(n=81449)
        result = evaluate(make_rec_list(), catalog)
        assert result["catalog_count"] == 81449

    def test_null_genre_values_handled(self):
        from ml.evaluation.model_bias import evaluate
        recs = make_rec_list()
        recs[0]["genre"] = None
        recs[1]["genre"] = None
        result = evaluate(recs, make_catalog())
        # Should not raise; "unknown" should appear in genre proportions
        genre_props = result["genre_representation"]["recommendation_proportions"]
        assert "unknown" in genre_props

    def test_overall_bias_is_bool(self):
        from ml.evaluation.model_bias import evaluate
        result = evaluate(make_rec_list(), make_catalog())
        assert isinstance(result["overall_bias_detected"], bool)

    def test_mitigation_suggestions_is_list(self):
        from ml.evaluation.model_bias import evaluate
        result = evaluate(make_rec_list(), make_catalog())
        assert isinstance(result["mitigation_suggestions"], list)

    def test_does_not_mutate_input_list(self):
        from ml.evaluation.model_bias import evaluate
        recs = make_rec_list()
        original_len = len(recs)
        original_first = dict(recs[0])
        evaluate(recs, make_catalog())
        assert len(recs) == original_len
        assert recs[0] == original_first

    def test_result_is_json_serializable(self):
        """
        The result must be JSON-serializable because write_bias_snapshot()
        writes it directly to Firestore.
        """
        import json
        from ml.evaluation.model_bias import evaluate
        result = evaluate(make_rec_list(), make_catalog())
        try:
            json.dumps(result)
        except (TypeError, ValueError) as e:
            pytest.fail(f"evaluate() result is not JSON-serializable: {e}")


# ── _empty_bias_report() tests ────────────────────────────────────────────────

class TestEmptyBiasReport:
    """
    _empty_bias_report() is the fallback returned by evaluate() when it
    cannot run. Its shape must be identical to generate_bias_report() so
    callers never need to branch on the return value.
    """

    def test_returns_dict(self):
        from ml.evaluation.model_bias import _empty_bias_report
        assert isinstance(_empty_bias_report(), dict)

    def test_has_all_top_level_keys(self):
        from ml.evaluation.model_bias import _empty_bias_report
        result = _empty_bias_report()
        assert EXPECTED_TOP_LEVEL_KEYS.issubset(result.keys())

    def test_genre_representation_has_all_keys(self):
        from ml.evaluation.model_bias import _empty_bias_report
        result = _empty_bias_report()
        assert EXPECTED_GENRE_REP_KEYS.issubset(result["genre_representation"].keys())

    def test_score_disparity_has_all_keys(self):
        from ml.evaluation.model_bias import _empty_bias_report
        result = _empty_bias_report()
        assert EXPECTED_SCORE_DISP_KEYS.issubset(result["score_disparity"].keys())

    def test_overall_bias_detected_is_false(self):
        from ml.evaluation.model_bias import _empty_bias_report
        assert _empty_bias_report()["overall_bias_detected"] is False

    def test_mitigation_suggestions_is_list_with_message(self):
        from ml.evaluation.model_bias import _empty_bias_report
        result = _empty_bias_report()
        assert isinstance(result["mitigation_suggestions"], list)
        assert len(result["mitigation_suggestions"]) > 0

    def test_disparity_threshold_matches_constant(self):
        from ml.evaluation.model_bias import _empty_bias_report
        result = _empty_bias_report()
        assert result["score_disparity"]["disparity_threshold"] == SCORE_DISPARITY_THRESHOLD

    def test_is_json_serializable(self):
        import json
        from ml.evaluation.model_bias import _empty_bias_report
        try:
            json.dumps(_empty_bias_report())
        except (TypeError, ValueError) as e:
            pytest.fail(f"_empty_bias_report() is not JSON-serializable: {e}")

    def test_shape_matches_generate_bias_report(self):
        """
        The top-level keys of _empty_bias_report must be a superset of
        generate_bias_report's keys so downstream code never gets KeyErrors.
        """
        from ml.evaluation.model_bias import generate_bias_report, _empty_bias_report
        real = generate_bias_report(make_recommendations(), make_catalog())
        empty = _empty_bias_report()
        missing = set(real.keys()) - set(empty.keys())
        assert not missing, f"_empty_bias_report() is missing keys: {missing}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
