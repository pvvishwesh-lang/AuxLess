import io
import json
import logging
import math
import numpy as np
import pandas as pd
from google.cloud import storage

logger = logging.getLogger(__name__)

UNDERREPRESENTED_THRESHOLD = 0.05
DOMINANCE_THRESHOLD        = 0.60


def _load_df(bucket_name: str, blob_path: str) -> pd.DataFrame:
    client  = storage.Client()
    content = client.bucket(bucket_name).blob(blob_path).download_as_text()
    return pd.read_csv(io.StringIO(content))


def _save_df(df: pd.DataFrame, bucket_name: str, blob_path: str):
    client = storage.Client()
    client.bucket(bucket_name).blob(blob_path).upload_from_string(
        df.to_csv(index=False),
        content_type="text/csv"
    )
    logger.info(f"Saved mitigated dataset to gs://{bucket_name}/{blob_path}")


def upsample_underrepresented(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col not in df.columns:
        logger.warning(f"Column '{col}' not found, skipping upsampling.")
        return df

    counts     = df[col].fillna("Unknown").value_counts()
    total      = len(df)
    frames     = [df]

    for label, count in counts.items():
        proportion = count / total
        if proportion < UNDERREPRESENTED_THRESHOLD:
            needed = math.ceil(
                (UNDERREPRESENTED_THRESHOLD * total - count) / (1 - UNDERREPRESENTED_THRESHOLD)
            )
            if needed > 0:
                slice_df = df[df[col].fillna("Unknown") == label]
                extra    = slice_df.sample(n=needed, replace=True, random_state=42)
                frames.append(extra)
                logger.info(f"Upsampled '{label}' in '{col}': {count} → {count + needed} rows")

    result = pd.concat(frames, ignore_index=True)
    logger.info(f"Dataset size after upsampling '{col}': {len(df)} → {len(result)}")
    return result


def downsample_dominant(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col not in df.columns:
        logger.warning(f"Column '{col}' not found, skipping downsampling.")
        return df

    counts = df[col].fillna("Unknown").value_counts()
    total  = len(df)
    frames = []

    for label, count in counts.items():
        slice_df = df[df[col].fillna("Unknown") == label]
        if count / total > DOMINANCE_THRESHOLD:
            non_dominant = total - count
            target_max   = int(non_dominant * DOMINANCE_THRESHOLD / (1 - DOMINANCE_THRESHOLD))
            slice_df = slice_df.sample(n=target_max, replace=False, random_state=42)
            logger.info(f"Downsampled '{label}' in '{col}': {count} → {target_max} rows")
        frames.append(slice_df)
    result = pd.concat(frames, ignore_index=True)
    logger.info(f"Dataset size after downsampling '{col}': {len(df)} → {len(result)}")
    return result


def compute_mitigation_report(
    df_before: pd.DataFrame,
    df_after: pd.DataFrame,
    slice_cols: list
) -> dict:
    report = {}
    for col in slice_cols:
        if col not in df_before.columns:
            continue

        before_props = (
            df_before[col].fillna("Unknown").value_counts(normalize=True).round(4).to_dict()
        )
        after_props = (
            df_after[col].fillna("Unknown").value_counts(normalize=True).round(4).to_dict()
        )

        mitigated = [
            label for label in before_props
            if before_props.get(label, 0) < UNDERREPRESENTED_THRESHOLD
            and after_props.get(label, 0) >= UNDERREPRESENTED_THRESHOLD
        ]
        reduced = [
            label for label in before_props
            if before_props.get(label, 0) > DOMINANCE_THRESHOLD
            and after_props.get(label, 0) <= DOMINANCE_THRESHOLD
        ]

        report[col] = {
            "before":         before_props,
            "after":          after_props,
            "rows_before":    len(df_before),
            "rows_after":     len(df_after),
            "upsampled":      mitigated,
            "downsampled":    reduced,
            "trade_off_note": (
                "Upsampling duplicates minority rows — model may overfit to these samples. "
                "Downsampling removes majority rows — overall dataset size is reduced. "
                "Both techniques improve representation equity at the cost of data volume fidelity."
            )
        }

    return report


def run_bias_mitigation(bucket_name: str, session_id: str, slice_cols: list) -> dict:
    input_path  = f"Final_Output/{session_id}_combined_valid.csv"
    output_path = f"Final_Output/{session_id}_mitigated.csv"
    report_path = f"Final_Output/{session_id}_mitigation_report.json"

    logger.info(f"Loading data for bias mitigation: gs://{bucket_name}/{input_path}")
    df_original = _load_df(bucket_name, input_path)
    df          = df_original.copy()

    for col in slice_cols:
        if col not in df.columns:
            logger.warning(f"Column '{col}' not found, skipping mitigation for it.")
            continue
        df = upsample_underrepresented(df, col)
        df = downsample_dominant(df, col)

    report = compute_mitigation_report(df_original, df, slice_cols)
    _save_df(df, bucket_name, output_path)

    client = storage.Client()
    client.bucket(bucket_name).blob(report_path).upload_from_string(
        json.dumps(report, indent=2),
        content_type="application/json"
    )
    logger.info(f"Mitigation report saved to gs://{bucket_name}/{report_path}")
    return report
