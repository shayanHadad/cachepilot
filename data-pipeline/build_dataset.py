"""
Turns raw JSONL request logs (data/raw_logs/run_*/service.jsonl) into
a feature table for model training.

The windowing math here (frequency_1min, frequency_5min, recency_sec,
inter_arrival_avg) mirrors go-cache-service/internal/features/tracker.go
on purpose, line for line — the model has to be trained on features
computed the same way they'll be computed at inference time, or
there's a systematic mismatch between training and serving.

payload_size_kb comes from MongoDB's posts.media_size_kb, not the
log's response_size — response_size is the full serialized JSON size
(always > 0, even for text posts), while the ML service actually sees
the post's own media_size_kb field (0 for text posts). Using
response_size here would train on a feature the model never actually
sees at inference time.

Usage:
    pip install pymongo pandas --break-system-packages
    python build_dataset.py --logs "../data/raw_logs/run_*/service.jsonl" \
        --out ../data/datasets/features.parquet
"""

import argparse
import glob
import json

import pandas as pd
from pymongo import MongoClient

FIVE_MIN_SEC = 300
ONE_MIN_SEC = 60


def load_records(log_glob: str) -> list[dict]:
    """Reads every matching JSONL file and returns records sorted by
    timestamp — the windowing logic below assumes strictly increasing
    time order across the whole dataset, not just within one file.

    Skips lines that fail to parse instead of crashing the whole
    pipeline over one bad line. This happens in practice: if a log
    snapshot gets copied at the exact moment the Go service is
    mid-write to it, or the service was force-killed instead of
    shut down cleanly, the last line can end up truncated.
    """
    records = []
    skipped = 0
    for path in sorted(glob.glob(log_glob)):
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    skipped += 1
    if skipped:
        print(f"Warning: skipped {skipped} malformed line(s) while reading logs")
    if not records:
        raise RuntimeError(f"No records found matching {log_glob}")
    records.sort(key=lambda r: r["timestamp"])
    return records


def load_media_sizes(mongo_uri: str, db_name: str, keys: set[str]) -> dict[str, float]:
    """Fetches media_size_kb for the given post ids in one batch query
    — this is the real field the ML service sees at inference time
    (see module docstring for why it's not derived from the log)."""
    from bson import ObjectId

    client = MongoClient(mongo_uri)
    object_ids = [ObjectId(k) for k in keys]
    docs = client[db_name]["posts"].find(
        {"_id": {"$in": object_ids}}, {"media_size_kb": 1}
    )
    return {str(doc["_id"]): doc.get("media_size_kb", 0.0) for doc in docs}


def prune(timestamps: list[float], now: float) -> list[float]:
    """Drops entries older than the 5-minute window, same as Go's
    prune() in tracker.go. timestamps must be sorted ascending."""
    cutoff = now - FIVE_MIN_SEC
    idx = 0
    while idx < len(timestamps) and timestamps[idx] < cutoff:
        idx += 1
    return timestamps[idx:]


def compute_stats(prior: list[float], now: float) -> dict:
    """Mirrors Go's computeStats(): derives window stats from a key's
    history *before* the current access, using only what a live
    tracker would have known at that exact moment — not information
    from later in the log, which would leak future data into a
    training feature."""
    pruned = prune(prior, now)
    if not pruned:
        return {
            "frequency_1min": 0,
            "frequency_5min": 0,
            "recency_sec": 0.0,
            "inter_arrival_avg": 0.0,
        }

    one_min_ago = now - ONE_MIN_SEC
    freq1 = sum(1 for t in pruned if t > one_min_ago)
    freq5 = len(pruned)
    recency = now - pruned[-1]

    if len(pruned) >= 2:
        diffs = [pruned[i] - pruned[i - 1] for i in range(1, len(pruned))]
        inter_arrival = sum(diffs) / len(diffs)
    else:
        inter_arrival = 0.0

    return {
        "frequency_1min": freq1,
        "frequency_5min": freq5,
        "recency_sec": recency,
        "inter_arrival_avg": inter_arrival,
    }


def build_features(records: list[dict], media_sizes: dict[str, float]) -> pd.DataFrame:
    history: dict[str, list[float]] = {}
    rows = []

    for rec in records:
        key = rec["key"]
        now = rec["timestamp"] / 1000.0  # ms -> sec, same units as the Go side

        prior = history.get(key, [])
        stats = compute_stats(prior, now)

        rows.append({
            "key": key,
            "timestamp": rec["timestamp"],
            **stats,
            "payload_size_kb": media_sizes.get(key, 0.0),
            "query_type": rec["query_type"],
        })

        updated = prior + [now]
        history[key] = prune(updated, now)

    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs", default="../data/raw_logs/run_*/service.jsonl")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--mongo-db", default="cachepilot")
    parser.add_argument("--out", default="../data/datasets/features.parquet")
    args = parser.parse_args()

    records = load_records(args.logs)
    print(f"Loaded {len(records)} log records from {args.logs}")

    unique_keys = {r["key"] for r in records}
    media_sizes = load_media_sizes(args.mongo_uri, args.mongo_db, unique_keys)
    print(f"Fetched media_size_kb for {len(media_sizes)} of {len(unique_keys)} unique keys")

    df = build_features(records, media_sizes)

    import os
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_parquet(args.out, index=False)
    print(f"Wrote {len(df)} rows to {args.out}")


if __name__ == "__main__":
    main()