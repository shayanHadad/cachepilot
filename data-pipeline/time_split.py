"""
Splits the labeled dataset into train/test sets by time — the last
train_fraction of rows (by timestamp) go to train, the rest to test.

Not a random split: a random split would let the model train on rows
that happened *after* some of its test rows, which isn't a situation
the model will ever actually be in at inference time (it only ever
sees the past). Splitting by time keeps the evaluation honest about
how the model would perform on genuinely unseen future data.

Usage:
    python time_split.py --in ../data/datasets/labeled.parquet \
        --train-out ../data/datasets/train.parquet \
        --test-out ../data/datasets/test.parquet \
        --train-fraction 0.8
"""

import argparse

import pandas as pd


def time_split(df: pd.DataFrame, train_fraction: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.sort_values("timestamp").reset_index(drop=True)
    split_idx = int(len(df) * train_fraction)
    return df.iloc[:split_idx], df.iloc[split_idx:]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--in", dest="input_path", default="../data/datasets/labeled.parquet")
    parser.add_argument("--train-out", default="../data/datasets/train.parquet")
    parser.add_argument("--test-out", default="../data/datasets/test.parquet")
    parser.add_argument("--train-fraction", type=float, default=0.8)
    args = parser.parse_args()

    df = pd.read_parquet(args.input_path)
    train_df, test_df = time_split(df, args.train_fraction)

    train_df.to_parquet(args.train_out, index=False)
    test_df.to_parquet(args.test_out, index=False)

    print(f"Train: {len(train_df)} rows ({train_df['timestamp'].min()} to {train_df['timestamp'].max()})")
    print(f"Test:  {len(test_df)} rows ({test_df['timestamp'].min()} to {test_df['timestamp'].max()})")
    print(f"Train label_admit rate: {train_df['label_admit'].mean():.2%}")
    print(f"Test label_admit rate:  {test_df['label_admit'].mean():.2%}")


if __name__ == "__main__":
    main()