"""
Checks that scoring one request at a time (what the gRPC server does)
gives the same answers as scoring the whole test set in one batch (what
train.py does), and measures how long a single decision takes.

It covers three things:
  1. parity: single-request probabilities and decisions vs. batch scoring,
     which also proves query_type is encoded the same way in both paths;
  2. an unknown query_type is rejected instead of silently scored;
  3. in-process latency of decide(), to compare against the Go side's
     ML timeout. gRPC and network time are not included.

Usage:
    python model/check_inference.py
"""

import argparse
import json
import sys
import time
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

ML_SERVICE_DIR = Path(__file__).resolve().parent.parent
REPO_DIR = ML_SERVICE_DIR.parent
sys.path.insert(0, str(ML_SERVICE_DIR))

from features.feature_engineering import FEATURE_COLUMNS, prepare_features
from model import inference

PROB_TOLERANCE = 1e-9
WARMUP_CALLS = 50


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default=str(ML_SERVICE_DIR / "model" / "artifacts" / "model.txt"))
    parser.add_argument("--test", default=str(REPO_DIR / "data" / "datasets" / "test.parquet"))
    parser.add_argument("--latency-rows", type=int, default=2000,
                         help="How many rows to time decide() on")
    parser.add_argument("--budget-ms", type=float, default=8.0,
                         help="The Go side's ML timeout; used only to flag slow runs")
    return parser.parse_args()


def check_parity(model_path: Path, records: list, test_df: pd.DataFrame) -> bool:
    meta = json.loads(model_path.with_name(model_path.stem + "_meta.json").read_text(encoding="utf-8"))
    booster = lgb.Booster(model_file=str(model_path))
    batch_p = booster.predict(prepare_features(test_df), num_iteration=meta["best_iteration"])
    threshold = float(meta["reject_score_threshold"])
    batch_admit = ~((1.0 - batch_p) >= threshold)

    single_p = np.array([inference.admit_probability(**rec) for rec in records])
    single_admit = np.array([
        inference.decide("", **rec).admit for rec in records
    ])

    max_diff = float(np.max(np.abs(single_p - batch_p)))
    mismatches = int(np.sum(single_admit != batch_admit))
    counts = test_df["query_type"].value_counts().to_dict()

    print("--- Parity: single-request vs batch scoring ---")
    print(f"  rows checked:           {len(records)}  (query_type counts: {counts})")
    print(f"  max |p_single - p_batch|: {max_diff:.3e}  (tolerance {PROB_TOLERANCE:.0e})")
    print(f"  decision mismatches:      {mismatches}")
    ok = max_diff <= PROB_TOLERANCE and mismatches == 0
    print(f"  {'PASS' if ok else 'FAIL'}")
    return ok


def check_unknown_query_type(record: dict) -> bool:
    print("\n--- Unknown query_type is rejected ---")
    bad = dict(record, query_type="video_post")
    try:
        inference.decide("", **bad)
    except inference.InvalidRequestError as e:
        print(f"  raised InvalidRequestError: {e}")
        print("  PASS")
        return True
    print("  no error raised for an unseen query_type")
    print("  FAIL")
    return False


def check_latency(records: list, rows: int, budget_ms: float) -> bool:
    sample = records[: max(rows, WARMUP_CALLS + 1)]
    for rec in sample[:WARMUP_CALLS]:
        inference.decide("", **rec)

    times_ms = []
    for rec in sample[WARMUP_CALLS:]:
        start = time.perf_counter()
        inference.decide("", **rec)
        times_ms.append((time.perf_counter() - start) * 1000.0)

    p50, p95, p99 = np.percentile(times_ms, [50, 95, 99])
    print(f"\n--- In-process decide() latency over {len(times_ms)} calls ---")
    print(f"  p50 {p50:.3f} ms   p95 {p95:.3f} ms   p99 {p99:.3f} ms   max {max(times_ms):.3f} ms")
    print(f"  Go's ML timeout is {budget_ms:.0f} ms and also covers the gRPC round trip")
    ok = p99 <= budget_ms / 2
    print(f"  {'OK' if ok else 'WARN: p99 uses more than half of the timeout budget'}")
    return ok


def main() -> None:
    args = parse_args()
    model_path = Path(args.model)

    try:
        inference.init("model", model_path)
    except inference.InferenceInitError as e:
        sys.exit(f"cannot start: {e}")
    print(f"Active decider: {inference.describe()}\n")

    test_df = pd.read_parquet(args.test)
    records = test_df[FEATURE_COLUMNS].to_dict("records")

    parity_ok = check_parity(model_path, records, test_df)
    unknown_ok = check_unknown_query_type(records[0])
    latency_ok = check_latency(records, args.latency_rows, args.budget_ms)

    # Latency is advisory: only parity and error handling fail the run.
    if not (parity_ok and unknown_ok):
        sys.exit(1)
    if not latency_ok:
        print("\nParity passed, but latency is close to the budget; see the warning above.")


if __name__ == "__main__":
    main()