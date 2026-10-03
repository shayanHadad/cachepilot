"""
Adds look-ahead labels to the feature table build_dataset.py
produces.

label_admit answers: "was this key requested again within
label_window_sec after this access?" — computed by looking *forward*
in the log, which is exactly why this has to be a separate pass from
build_dataset.py (whose features only look backward, to match what's
actually available at inference time). Mixing look-ahead and
look-back logic in one function would make it too easy to
accidentally leak a look-ahead value into a feature.

Usage:
    python labeling.py --in ../data/datasets/features.parquet \
        --out ../data/datasets/labeled.parquet --window-sec 300
"""

import argparse

import pandas as pd


def add_labels(df: pd.DataFrame, window_sec: float) -> pd.DataFrame:
    # Sort by (key, timestamp) so shift(-1) within each group gives
    # each row's *next* access to the same key, in chronological
    # order — not just the next row in the file.
    df = df.sort_values(["key", "timestamp"]).reset_index(drop=True)
    min_ts = df["timestamp"].min()

    df["next_ts"] = df.groupby("key")["timestamp"].shift(-1)
    df["time_to_next_sec"] = (df["next_ts"] - df["timestamp"]) / 1000.0

    # No next access at all (this was the last time the key was seen
    # in the log) counts as "not admitted" — there's nothing to
    # justify caching it looking forward, at least within the data we have.
    df["label_admit"] = (
        df["time_to_next_sec"].notna() & (df["time_to_next_sec"] <= window_sec)
    ).astype(int)

    # Optional regression target: seconds until the next request.
    # Left as NaN when there's no next access — leave it to whoever
    # trains on this to decide how to handle that (drop, clip, treat
    # as censored), rather than picking a value here that would look
    # like real data.
    df = df.drop(columns=["next_ts"]).rename(columns={"time_to_next_sec": "label_ttl_sec"})

    # Rows within window_sec of the log's own end can't have a
    # trustworthy label_admit: a "no next access" there might just
    # mean the log stopped recording before the next request would
    # have happened, not that there really wasn't one (right-
    # censoring). Keeping these would systematically under-label
    # label_admit near the end of the log — and since time_split.py
    # puts the tail of the log into the test set, that bias would
    # land entirely in test, making it look artificially different
    # from train even though nothing about the underlying access
    # pattern actually changed. Dropping them is safer than keeping a
    # label we know is likely wrong.
    max_ts = df["timestamp"].max()
    cutoff = max_ts - window_sec * 1000
    before = len(df)
    df = df[df["timestamp"] <= cutoff].copy()
    dropped = before - len(df)
    if dropped:
        print(f"Dropped {dropped} row(s) within {window_sec:.0f}s of the log's end "
              f"(unreliable label_admit due to right-censoring)")

    if df.empty:
        span_sec = (max_ts - min_ts) / 1000.0
        raise RuntimeError(
            f"All rows were dropped — the log's total duration ({span_sec:.0f}s) "
            f"isn't longer than window_sec ({window_sec:.0f}s), so nothing is far "
            f"enough from the end to have a reliable label. Run a longer workload "
            f"(total_requests / requests_per_sec should be well above window_sec) "
            f"or lower --window-sec if a shorter look-ahead is acceptable."
        )

    return df.sort_values("timestamp").reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--in", dest="input_path", default="../data/datasets/features.parquet")
    parser.add_argument("--out", default="../data/datasets/labeled.parquet")
    parser.add_argument("--window-sec", type=float, default=300.0,
                         help="Look-ahead window for label_admit, in seconds (default: 5 minutes)")
    args = parser.parse_args()

    df = pd.read_parquet(args.input_path)
    print(f"Loaded {len(df)} rows from {args.input_path}")

    labeled = add_labels(df, args.window_sec)
    admit_rate = labeled["label_admit"].mean()
    print(f"label_admit positive rate: {admit_rate:.2%}")

    labeled.to_parquet(args.out, index=False)
    print(f"Wrote {len(labeled)} labeled rows to {args.out}")


if __name__ == "__main__":
    main()