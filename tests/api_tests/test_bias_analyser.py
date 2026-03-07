import pytest
import pandas as pd
from backend.pipelines.api.bias_analyser import compute_bias_metrics, _slice_stats


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "genre":   ["Pop"] * 70 + ["Rock"] * 20 + ["Jazz"] * 10,
        "country": ["USA"] * 60 + ["GBR"] * 25 + ["AUS"] * 15,
    })


class TestSliceStats:

    def test_counts_sum_to_total(self, sample_df):
        stats = _slice_stats(sample_df, "genre")
        assert sum(stats["counts"].values()) == stats["total"]

    def test_proportions_sum_to_one(self, sample_df):
        stats = _slice_stats(sample_df, "genre")
        assert sum(stats["proportions"].values()) == pytest.approx(1.0, abs=1e-3)

    def test_bias_flag_set_when_dominant_share_above_60pct(self, sample_df):
        assert _slice_stats(sample_df, "genre")["bias_flag"] is True  # Pop = 70%

    def test_bias_flag_not_set_when_balanced(self):
        df = pd.DataFrame({"genre": ["Pop"] * 34 + ["Rock"] * 33 + ["Jazz"] * 33})
        assert _slice_stats(df, "genre")["bias_flag"] is False

    def test_unknown_filled_for_nulls(self):
        df = pd.DataFrame({"genre": ["Pop", None, None, "Rock"]})
        assert "Unknown" in _slice_stats(df, "genre")["counts"]

    def test_entropy_is_zero_for_single_value(self):
        df = pd.DataFrame({"genre": ["Pop"] * 10})
        assert _slice_stats(df, "genre")["entropy"] == pytest.approx(0.0, abs=1e-4)

    def test_underrepresented_flags_small_slices(self):
        df = pd.DataFrame({"genre": ["Pop"] * 98 + ["Jazz"] * 1 + ["Rock"] * 1})
        assert "Jazz" in _slice_stats(df, "genre")["underrepresented"]


class TestComputeBiasMetrics:

    def test_returns_expected_keys(self, sample_df, tmp_path):
        csv_path = tmp_path / "test.csv"
        sample_df.to_csv(csv_path, index=False)
        result = compute_bias_metrics(str(csv_path), slice_cols=["genre", "country"])
        assert "slices"             in result
        assert "cross_slices"       in result
        assert "overall_bias_flags" in result
        assert "record_count"       in result

    def test_missing_column_skipped_gracefully(self, sample_df, tmp_path):
        csv_path = tmp_path / "test.csv"
        sample_df.to_csv(csv_path, index=False)
        result = compute_bias_metrics(str(csv_path), slice_cols=["genre", "nonexistent"])
        assert "genre"       in result["slices"]
        assert "nonexistent" not in result["slices"]

    def test_cross_slice_generated(self, sample_df, tmp_path):
        csv_path = tmp_path / "test.csv"
        sample_df.to_csv(csv_path, index=False)
        result = compute_bias_metrics(str(csv_path), slice_cols=["genre", "country"])
        assert "genre_x_country" in result["cross_slices"]

    def test_bias_flags_populated(self, sample_df, tmp_path):
        csv_path = tmp_path / "test.csv"
        sample_df.to_csv(csv_path, index=False)
        result = compute_bias_metrics(str(csv_path), slice_cols=["genre", "country"])
        assert "genre"   in result["overall_bias_flags"]
        assert "country" in result["overall_bias_flags"]
