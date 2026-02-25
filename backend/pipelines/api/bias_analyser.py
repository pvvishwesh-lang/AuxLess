import io
import logging
import numpy as np
import pandas as pd
from scipy.stats import entropy as scipy_entropy
from google.cloud import storage

logger = logging.getLogger(__name__)

def _load_df(csv_file: str) -> pd.DataFrame:
    if csv_file.startswith("gs://"):
        parts = csv_file[5:].split("/", 1)
        bucket_name, blob_path = parts[0], parts[1]
        client = storage.Client()
        content = client.bucket(bucket_name).blob(blob_path).download_as_text()
        return pd.read_csv(io.StringIO(content))
    return pd.read_csv(csv_file)

def _slice_stats(df: pd.DataFrame, col: str) -> dict:
    counts = df[col].fillna("Unknown").value_counts()
    total = counts.sum()
    proportions = counts / total
    return {
        "counts":          counts.to_dict(),
        "proportions":     proportions.round(4).to_dict(),
        "total":           int(total),
        "unique_values":   int(counts.nunique()),
        "entropy":         round(float(scipy_entropy(proportions)), 4),
        "max_entropy":     round(float(np.log(max(len(counts), 1))), 4),
        "dominant_label":  str(counts.index[0]),
        "dominant_share":  round(float(proportions.iloc[0]), 4),
        "underrepresented": [
            label for label, prop in proportions.items() if prop < 0.01
        ],
        "bias_flag": float(proportions.iloc[0]) > 0.6,
    }

def _cross_slice_stats(df: pd.DataFrame, col_a: str, col_b: str) -> dict:
    cross = pd.crosstab(
        df[col_a].fillna("Unknown"),
        df[col_b].fillna("Unknown"),
        normalize="index"
    )
    return cross.round(4).to_dict()

def compute_bias_metrics(csv_file: str, slice_cols: list) -> dict:
    df = _load_df(csv_file)
    logger.info(f"Computing bias on {len(df)} records, slices: {slice_cols}")

    summary = {
        "record_count":       len(df),
        "slices":             {},
        "cross_slices":       {},
        "overall_bias_flags": []
    }

    for col in slice_cols:
        if col not in df.columns:
            logger.warning(f"Slice column '{col}' not found, skipping.")
            continue
        summary["slices"][col] = _slice_stats(df, col)
        if summary["slices"][col]["bias_flag"]:
            summary["overall_bias_flags"].append(col)

    for i in range(len(slice_cols)):
        for j in range(i + 1, len(slice_cols)):
            a, b = slice_cols[i], slice_cols[j]
            if a in df.columns and b in df.columns:
                summary["cross_slices"][f"{a}_x_{b}"] = _cross_slice_stats(df, a, b)

    logger.info(f"Bias flags on: {summary['overall_bias_flags']}")
    return summary
