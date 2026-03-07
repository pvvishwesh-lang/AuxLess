import json
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from backend.pipelines.api.bias_mitigation import (
    upsample_underrepresented,
    downsample_dominant,
    compute_mitigation_report,
    run_bias_mitigation,
    UNDERREPRESENTED_THRESHOLD,
    DOMINANCE_THRESHOLD,
)


@pytest.fixture
def biased_df():
    return pd.DataFrame({
        "genre":       ["Pop"] * 70 + ["Rock"] * 28 + ["Jazz"] * 2,
        "country":     ["USA"] * 60 + ["GBR"] * 30 + ["AUS"] * 10,
        "track_title": [f"Track {i}" for i in range(100)],
    })


@pytest.fixture
def balanced_df():
    return pd.DataFrame({
        "genre":       ["Pop"] * 34 + ["Rock"] * 33 + ["Jazz"] * 33,
        "country":     ["USA"] * 34 + ["GBR"] * 33 + ["AUS"] * 33,
        "track_title": [f"Track {i}" for i in range(100)],
    })


class TestUpsampleUnderrepresented:

    def test_underrepresented_slice_is_grown(self, biased_df):
        result    = upsample_underrepresented(biased_df, "genre")
        jazz_count = (result["genre"] == "Jazz").sum()
        total      = len(result)
        assert jazz_count / total >= UNDERREPRESENTED_THRESHOLD

    def test_majority_slice_is_not_changed(self, biased_df):
        original_pop = (biased_df["genre"] == "Pop").sum()
        result       = upsample_underrepresented(biased_df, "genre")
        assert (result["genre"] == "Pop").sum() == original_pop

    def test_balanced_df_unchanged(self, balanced_df):
        result = upsample_underrepresented(balanced_df, "genre")
        assert len(result) == len(balanced_df)

    def test_output_has_more_rows_than_input(self, biased_df):
        result = upsample_underrepresented(biased_df, "genre")
        assert len(result) > len(biased_df)

    def test_missing_column_leaves_df_unchanged(self, biased_df):
        result = upsample_underrepresented(biased_df, "nonexistent_col")
        assert len(result) == len(biased_df)

    def test_null_values_treated_as_unknown(self):
        df     = pd.DataFrame({"genre": ["Pop"] * 95 + [None] * 5})
        result = upsample_underrepresented(df, "genre")
        null_or_unknown = (
            result["genre"].isna().sum() + (result["genre"] == "Unknown").sum()
        )
        assert null_or_unknown / len(result) >= UNDERREPRESENTED_THRESHOLD


class TestDownsampleDominant:

    def test_dominant_slice_is_reduced(self, biased_df):
        result    = downsample_dominant(biased_df, "genre")
        pop_share = (result["genre"] == "Pop").sum() / len(result)
        assert pop_share <= DOMINANCE_THRESHOLD

    def test_non_dominant_slices_unchanged(self, biased_df):
        original_rock = (biased_df["genre"] == "Rock").sum()
        result        = downsample_dominant(biased_df, "genre")
        assert (result["genre"] == "Rock").sum() == original_rock

    def test_balanced_df_unchanged(self, balanced_df):
        result = downsample_dominant(balanced_df, "genre")
        assert len(result) == len(balanced_df)

    def test_output_has_fewer_or_equal_rows(self, biased_df):
        result = downsample_dominant(biased_df, "genre")
        assert len(result) <= len(biased_df)

    def test_missing_column_leaves_df_unchanged(self, biased_df):
        result = downsample_dominant(biased_df, "nonexistent_col")
        assert len(result) == len(biased_df)


class TestComputeMitigationReport:

    def test_report_contains_all_slice_cols(self, biased_df, balanced_df):
        report = compute_mitigation_report(biased_df, balanced_df, ["genre", "country"])
        assert "genre"   in report
        assert "country" in report

    def test_report_has_before_and_after(self, biased_df, balanced_df):
        report = compute_mitigation_report(biased_df, balanced_df, ["genre"])
        assert "before"      in report["genre"]
        assert "after"       in report["genre"]
        assert "rows_before" in report["genre"]
        assert "rows_after"  in report["genre"]

    def test_upsampled_field_populated(self, biased_df):
        after_df = upsample_underrepresented(biased_df.copy(), "genre")
        report   = compute_mitigation_report(biased_df, after_df, ["genre"])
        assert "Jazz" in report["genre"]["upsampled"]

    def test_downsampled_field_populated(self, biased_df):
        after_df = downsample_dominant(biased_df.copy(), "genre")
        report   = compute_mitigation_report(biased_df, after_df, ["genre"])
        assert "Pop" in report["genre"]["downsampled"]

    def test_trade_off_note_present(self, biased_df, balanced_df):
        report = compute_mitigation_report(biased_df, balanced_df, ["genre"])
        assert "trade_off_note" in report["genre"]

    def test_missing_column_skipped(self, biased_df, balanced_df):
        report = compute_mitigation_report(biased_df, balanced_df, ["genre", "nonexistent"])
        assert "nonexistent" not in report


class TestRunBiasMitigation:

    def _make_mock_gcs(self, df: pd.DataFrame):
        mock_blob   = MagicMock()
        mock_blob.download_as_text.return_value = df.to_csv(index=False)
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        return mock_client

    def test_returns_report_with_expected_keys(self, biased_df):
        mock_client = self._make_mock_gcs(biased_df)
        with patch("backend.pipelines.api.bias_mitigation.storage.Client", return_value=mock_client):
            report = run_bias_mitigation("bucket", "sess_001", ["genre", "country"])
        assert "genre"   in report
        assert "country" in report

    def test_mitigated_csv_is_saved(self, biased_df):
        mock_client = self._make_mock_gcs(biased_df)
        with patch("backend.pipelines.api.bias_mitigation.storage.Client", return_value=mock_client):
            run_bias_mitigation("bucket", "sess_001", ["genre"])
        saved_paths = [call[0][0] for call in mock_client.bucket.return_value.blob.call_args_list]
        assert any("mitigated" in p for p in saved_paths)

    def test_mitigation_report_json_is_saved(self, biased_df):
        mock_client = self._make_mock_gcs(biased_df)
        with patch("backend.pipelines.api.bias_mitigation.storage.Client", return_value=mock_client):
            run_bias_mitigation("bucket", "sess_001", ["genre"])
        saved_paths = [call[0][0] for call in mock_client.bucket.return_value.blob.call_args_list]
        assert any("mitigation_report" in p for p in saved_paths)

    def test_missing_column_handled_gracefully(self, biased_df):
        mock_client = self._make_mock_gcs(biased_df)
        with patch("backend.pipelines.api.bias_mitigation.storage.Client", return_value=mock_client):
            report = run_bias_mitigation("bucket", "sess_001", ["nonexistent_col"])
        assert report == {}
