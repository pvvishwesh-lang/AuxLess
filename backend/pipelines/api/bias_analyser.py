import pandas as pd
from collections import Counter

def compute_bias_metrics(csv_file, slice_cols):
    df = pd.read_csv(csv_file)
    bias_summary = {}
    for col in slice_cols:
        counts = Counter(df[col].fillna("Unknown"))
        bias_summary[col] = dict(counts)
    return bias_summary