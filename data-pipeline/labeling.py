"""
Adds look-ahead labels to the feature table build_dataset.py produces.

label_reuse answers: "if this access put the key into an LRU cache that
holds `capacity` keys, would the key's next request still find it
there?" It is 1 when the key is requested again before `capacity` other
distinct keys have been requested, and 0 otherwise (the key never comes
back, or only after the cache would have turned over).

The label looks *forward* in the log, which is exactly why this is a
separate pass from build_dataset.py (whose features only look backward,
to match what's actually available at inference time). Mixing look-ahead
and look-back logic in one function would make it too easy to
accidentally leak a look-ahead value into a feature.

label_end_ts is the timestamp of the request at which the label became
certain: the key's next request, or the request that brought the number
of distinct keys since this access up to `capacity`. time_split.py uses
it to keep training labels from depending on the test period.

A request whose key never reappears, and which is followed by fewer than
`capacity` distinct keys, is dropped: the log may simply have stopped
before the key's next request (right-censoring).

Usage:
    python labeling.py --in ../data/datasets/features.parquet \
        --out ../data/datasets/labeled.parquet --capacity 250
"""

import argparse
from typing import NamedTuple

import numpy as np
import pandas as pd


class ReuseLabels(NamedTuple):
    label: np.ndarray       # 1 if the next request would hit an LRU cache of `capacity` keys
    end_index: np.ndarray   # index of the request at which the label became certain
    determined: np.ndarray  # False when the log ended before the label was certain
    next_index: np.ndarray  # index of the key's next request, -1 if none


def reuse_labels(keys: list, capacity: int) -> ReuseLabels:
    """Labels every request in `keys` (in request order) in one pass."""
    if capacity < 1:
        raise ValueError(f"capacity must be >= 1, got {capacity}")
    n = len(keys)

    next_index = np.full(n, -1, dtype=np.int64)
    last_seen: dict = {}
    for i in range(n - 1, -1, -1):
        next_index[i] = last_seen.get(keys[i], -1)
        last_seen[keys[i]] = i

    # full_at[i] is the index of the request that brought the number of
    # distinct keys since request i up to `capacity`, or -1 if the log ends
    # first. It never decreases as i grows, so a sliding window finds it.
    full_at = np.full(n, -1, dtype=np.int64)
    counts: dict = {}
    distinct = 0
    right = 0  # the window is keys[i + 1 : right]
    for i in range(n):
        right = max(right, i + 1)
        while distinct < capacity and right < n:
            key = keys[right]
            counts[key] = counts.get(key, 0) + 1
            if counts[key] == 1:
                distinct += 1
            right += 1
        if distinct >= capacity:
            full_at[i] = right - 1
        if right > i + 1:
            key = keys[i + 1]
            counts[key] -= 1
            if counts[key] == 0:
                del counts[key]
                distinct -= 1

    has_next = next_index >= 0
    reached = full_at >= 0
    # The next request comes before the capacity-th distinct key (or at it,
    # which counts the key itself) when it is not later than full_at.
    label = has_next & (~reached | (next_index <= full_at))
    determined = has_next | reached
    end_index = np.where(label, next_index, full_at)
    return ReuseLabels(label.astype(np.int64), end_index, determined, next_index)


def add_labels(df: pd.DataFrame, capacity: int) -> pd.DataFrame:
    # Request order is timestamp order; a stable sort keeps ties as logged.
    df = df.sort_values("timestamp", kind="stable").reset_index(drop=True)
    timestamps = df["timestamp"].to_numpy()

    labels = reuse_labels(df["key"].tolist(), capacity)

    df["label_reuse"] = labels.label
    df["label_end_ts"] = timestamps[np.where(labels.determined, labels.end_index, 0)]
    df["label_capacity"] = capacity

    # Optional regression target: seconds until the key's next request.
    # Left as NaN when there is none — leave it to whoever trains on this
    # to decide how to handle that, rather than picking a value here that
    # would look like real data.
    has_next = labels.next_index >= 0
    seconds = (timestamps[np.where(has_next, labels.next_index, 0)] - timestamps) / 1000.0
    df["label_ttl_sec"] = np.where(has_next, seconds, np.nan)

    before = len(df)
    df = df[labels.determined].reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        print(f"Dropped {dropped} row(s) at the end of the log: the key does not reappear "
              f"and fewer than {capacity} distinct keys follow (right-censoring)")

    if df.empty:
        raise RuntimeError(
            f"All rows were dropped — the log has fewer than {capacity} distinct keys after "
            f"every request that is not requested again. Record a longer workload or lower "
            f"--capacity."
        )
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--in", dest="input_path", default="../data/datasets/features.parquet")
    parser.add_argument("--out", default="../data/datasets/labeled.parquet")
    parser.add_argument("--capacity", type=int, default=250,
                        help="Cache capacity the label refers to (go-cache-service cache.capacity)")
    args = parser.parse_args()

    df = pd.read_parquet(args.input_path)
    print(f"Loaded {len(df)} rows from {args.input_path}")

    labeled = add_labels(df, args.capacity)
    print(f"label_reuse positive rate: {labeled['label_reuse'].mean():.2%}")

    labeled.to_parquet(args.out, index=False)
    print(f"Wrote {len(labeled)} labeled rows to {args.out}")


if __name__ == "__main__":
    main()