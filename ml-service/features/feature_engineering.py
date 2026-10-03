"""
Shared feature definitions for the admit/TTL model.

Having this in one place (rather than repeating column names and
dtype handling in train.py and wherever else reads these parquet
files) means training and any future scoring/evaluation code can't
quietly drift out of sync on which columns are used or how query_type
gets encoded.
"""

import pandas as pd

FEATURE_COLUMNS = [
    "frequency_1min",
    "frequency_5min",
    "recency_sec",
    "inter_arrival_avg",
    "payload_size_kb",
    "query_type",
]

CATEGORICAL_COLUMNS = ["query_type"]


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    """Selects the feature columns and casts categoricals to pandas'
    category dtype, which LightGBM uses natively — no one-hot or
    label encoding needed."""
    df = df[FEATURE_COLUMNS].copy()
    for col in CATEGORICAL_COLUMNS:
        df[col] = df[col].astype("category")
    return df