import io
import json
import logging
import pandas as pd
from google.cloud import storage

logger = logging.getLogger(__name__)

EXPECTED_SCHEMA = {
    "playlist_name":         {"nullable": False},
    "track_title":           {"nullable": False},
    "artist_name":           {"nullable": True},
    "video_id":              {"nullable": False},
    "genre":                 {"nullable": True},
    "country":               {"nullable": True},
    "collection_name":       {"nullable": True},
    "collection_id":         {"nullable": True},
    "trackTimeMillis":       {"nullable": False, "numeric": True, "min": 0},
    "trackTimeSeconds":      {"nullable": False, "numeric": True, "min": 0},
    "view_count":            {"nullable": False, "numeric": True, "min": 0},
    "like_count":            {"nullable": False, "numeric": True, "min": 0},
    "comment_count":         {"nullable": False, "numeric": True, "min": 0},
    "like_to_view_ratio":    {"nullable": False, "numeric": True, "min": 0.0, "max": 1.0},
    "comment_to_view_ratio": {"nullable": False, "numeric": True, "min": 0.0},
}


def _load_df(bucket_name: str, blob_path: str) -> pd.DataFrame:
    client = storage.Client()
    content = client.bucket(bucket_name).blob(blob_path).download_as_text()
    return pd.read_csv(io.StringIO(content))


def _generate_statistics(df: pd.DataFrame) -> dict:
    report = {}
    for col in df.columns:
        col_stats = {
            "missing_count": int(df[col].isna().sum()),
            "missing_pct":   round(df[col].isna().mean() * 100, 2),
            "unique_count":  int(df[col].nunique()),
        }
        if pd.api.types.is_numeric_dtype(df[col]):
            col_stats.update({
                "mean": round(float(df[col].mean()), 4),
                "std":  round(float(df[col].std()),  4),
                "min":  round(float(df[col].min()),  4),
                "max":  round(float(df[col].max()),  4),
                "p25":  round(float(df[col].quantile(0.25)), 4),
                "p75":  round(float(df[col].quantile(0.75)), 4),
            })
        report[col] = col_stats
    return report


def _validate_schema(df: pd.DataFrame) -> list:
    violations = []
    for col, rules in EXPECTED_SCHEMA.items():
        if col not in df.columns:
            violations.append({"column": col, "issue": "missing_column"})
            continue
        if not rules.get("nullable") and df[col].isna().any():
            violations.append({
                "column":     col,
                "issue":      "unexpected_nulls",
                "null_count": int(df[col].isna().sum())
            })
        if rules.get("numeric") and pd.api.types.is_numeric_dtype(df[col]):
            if "min" in rules:
                below = int((df[col] < rules["min"]).sum())
                if below:
                    violations.append({"column": col, "issue": f"below_min_{rules['min']}", "count": below})
            if "max" in rules:
                above = int((df[col] > rules["max"]).sum())
                if above:
                    violations.append({"column": col, "issue": f"above_max_{rules['max']}", "count": above})
    return violations


def run_schema_validation(bucket_name: str, session_id: str) -> dict:
    blob_path = f"Final_Output/{session_id}_combined_valid.csv"
    logger.info(f"Loading gs://{bucket_name}/{blob_path} for schema validation")
    df = _load_df(bucket_name, blob_path)

    stats      = _generate_statistics(df)
    violations = _validate_schema(df)

    result = {
        "session_id":        session_id,
        "row_count":         len(df),
        "column_count":      len(df.columns),
        "schema_valid":      len(violations) == 0,
        "schema_violations": violations,
        "statistics":        stats,
    }
    logger.info(f"Schema validation done. Valid: {result['schema_valid']}, Violations: {len(violations)}")
    return result
