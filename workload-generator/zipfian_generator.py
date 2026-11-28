"""
Sends a Zipfian-distributed request workload at go-cache-service,
using real post ids from MongoDB. Burst mode is optional — when on, a
few idle posts each take a fixed share of the traffic for a while (see
docs/architecture.md for why this is a toggle, not always-on).

The request sequence comes from workload_plan.build_plan and does not
depend on the wall clock. Each run gets its own timestamped folder with
results, config, burst ground truth and a snapshot of the Go service's
log — see create_run_dir.

Usage:
    pip install pymongo numpy requests
    python zipfian_generator.py --total-requests 5000 --requests-per-sec 50
    python zipfian_generator.py --enable-burst --total-requests 5000
    python zipfian_generator.py --keep-alive   # reuse one HTTP connection
    python zipfian_generator.py --enable-burst --dry-run   # plan only, needs numpy only
"""

import argparse
import json
import shutil
import time
from dataclasses import asdict
from pathlib import Path

import numpy as np

from config import WorkloadConfig
from workload_plan import Plan, build_plan, hash_post_ids


def load_post_ids(mongo_uri: str, db_name: str) -> list[str]:
    """All post ids in Mongo — the only valid keys the service can serve."""
    from pymongo import MongoClient  # imported here so --dry-run needs only numpy

    client = MongoClient(mongo_uri)
    ids = [str(doc["_id"]) for doc in client[db_name]["posts"].find({}, {"_id": 1})]
    if not ids:
        raise RuntimeError(
            f"No posts found in {db_name}.posts — run scripts/seed_posts.py first."
        )
    return ids


def print_plan_summary(plan: Plan, cfg: WorkloadConfig, n_posts: int) -> None:
    """Prints the plan's size, fingerprint and burst table."""
    total = len(plan.keys)
    print(f"Plan: {total} requests over {n_posts} posts, sha256 {plan.sha256[:16]}")
    if not cfg.enable_burst:
        print("Bursts: off")
        return

    target = cfg.burst_share * cfg.burst_duration_sec * cfg.requests_per_sec
    print(f"Bursts: {len(plan.bursts)}, {plan.burst_requests} burst requests "
          f"({plan.burst_requests / max(total, 1):.2%} of traffic), "
          f"target about {target:.0f} per burst")
    if plan.bursts:
        print("  id  start(s)  end(s)  rank  requests  share-in-window")
        for b in plan.bursts:
            window = b.end_index - b.start_index + 1
            print(f"  {b.burst_id:2d}  {b.start_index / cfg.requests_per_sec:8.1f}  "
                  f"{(b.end_index + 1) / cfg.requests_per_sec:6.1f}  {b.key_index + 1:4d}  "
                  f"{b.requests:8d}  {b.requests / window:15.1%}")


def create_run_dir(cfg: WorkloadConfig) -> Path:
    """Makes a fresh, uniquely named output folder for this run
    (timestamp + burst on/off), so runs never overwrite each other —
    no more manually mv-ing files between runs."""
    timestamp = time.strftime("%Y-%m-%d_%H%M%S")
    tag = "burst" if cfg.enable_burst else "normal"
    run_dir = Path(cfg.output_root) / f"run_{timestamp}_{tag}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def write_run_metadata(run_dir: Path, cfg: WorkloadConfig, extra: dict) -> None:
    """Dumps every parameter for this run, plus the plan and post list
    fingerprints, to run_config.json, so nobody has to remember what
    flags produced a given result."""
    metadata = asdict(cfg)
    metadata.update(extra)
    metadata["run_started_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    with open(run_dir / "run_config.json", "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)


def dry_run(cfg: WorkloadConfig) -> None:
    """Builds and prints the plan on synthetic posts; sends and writes nothing."""
    plan = build_plan(cfg.dry_run_posts, cfg)
    print(f"Dry run on {cfg.dry_run_posts} synthetic posts (nothing is sent or written)")
    print_plan_summary(plan, cfg, cfg.dry_run_posts)


def run_workload(cfg: WorkloadConfig) -> None:
    import requests  # imported here so --dry-run needs only numpy

    post_ids = load_post_ids(cfg.mongo_uri, cfg.mongo_db)
    n = len(post_ids)
    print(f"Loaded {n} post ids from {cfg.mongo_db}.posts")

    plan = build_plan(n, cfg)
    print_plan_summary(plan, cfg, n)

    run_dir = create_run_dir(cfg)
    write_run_metadata(run_dir, cfg, {
        "n_posts": n,
        "post_ids_sha256": hash_post_ids(post_ids),
        "plan_sha256": plan.sha256,
    })
    print(f"Run output directory: {run_dir}")

    # One shared session keeps the HTTP connection open between requests;
    # the module-level functions open a new connection for every request.
    http = requests.Session() if cfg.keep_alive else requests

    bursts_by_start = {b.start_index: b for b in plan.bursts}
    results_file = open(run_dir / "results.jsonl", "a", encoding="utf-8")

    burst_gt_file = None
    if cfg.enable_burst:
        burst_gt_file = open(run_dir / "burst_ground_truth.jsonl", "a", encoding="utf-8")

    successes = 0
    failures = 0
    latencies_ms: list[float] = []

    start_time = time.monotonic()
    print(f"Starting workload: {cfg.total_requests} requests at {cfg.requests_per_sec}/s "
          f"(burst={'on' if cfg.enable_burst else 'off'}, keep-alive={'on' if cfg.keep_alive else 'off'})")

    progress_every = max(cfg.total_requests // 10, 1)

    for i in range(cfg.total_requests):
        burst = bursts_by_start.get(i)
        if burst is not None and burst_gt_file is not None:
            start_ts = int(time.time() * 1000)
            burst_gt_file.write(json.dumps({
                "start_ts": start_ts,
                "end_ts": start_ts + int(cfg.burst_duration_sec * 1000),
                "key": post_ids[burst.key_index],
                "burst_id": burst.burst_id,
                "rank": burst.key_index + 1,
                "start_index": burst.start_index,
                "end_index": burst.end_index,
                "requests": burst.requests,
            }) + "\n")

        key = post_ids[int(plan.keys[i])]
        burst_id = int(plan.burst_ids[i])

        req_start = time.monotonic()
        try:
            resp = http.get(cfg.service_url + "/get", params={"key": key}, timeout=5)
            latency_ms = (time.monotonic() - req_start) * 1000
            latencies_ms.append(latency_ms)
            if resp.status_code == 200:
                successes += 1
            else:
                failures += 1
        except requests.RequestException as e:
            latency_ms = (time.monotonic() - req_start) * 1000
            failures += 1
            print(f"request {i} failed: {e}")

        results_file.write(json.dumps({
            "timestamp": int(time.time() * 1000),
            "key": key,
            "latency_ms": round(latency_ms, 3),
            "is_burst": burst_id >= 0,
            "index": i,
            "burst_id": burst_id if burst_id >= 0 else None,
        }) + "\n")

        if (i + 1) % progress_every == 0:
            print(f"  [{i + 1}/{cfg.total_requests}] requests sent")

        # Sleep until the *scheduled* time for the next request
        # (not just sleep(1/rate) after each one) so timing doesn't
        # drift over the course of a long run.
        next_request_at = start_time + (i + 1) / cfg.requests_per_sec
        sleep_for = next_request_at - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)

    results_file.close()
    if burst_gt_file is not None:
        burst_gt_file.close()

    # Snapshot (copy, not move — the service might still be writing
    # to it) the Go service's log into this run's folder too.
    service_log = Path(cfg.go_service_log_path)
    if service_log.exists():
        shutil.copy2(service_log, run_dir / "service.jsonl")
    else:
        print(f"Warning: {service_log} not found — skipping service log snapshot. "
              f"Is the go-cache-service running with a matching logging.path?")

    elapsed = time.monotonic() - start_time
    print("\n--- Workload summary ---")
    print(f"Total requests:   {cfg.total_requests}")
    print(f"Successes:        {successes}")
    print(f"Failures:         {failures}")
    print(f"Elapsed:          {elapsed:.1f}s")

    if latencies_ms:
        p50, p95, p99 = np.percentile(latencies_ms, [50, 95, 99])
        avg = sum(latencies_ms) / len(latencies_ms)
        print(f"Latency avg/p50/p95/p99: {avg:.2f} / {p50:.2f} / {p95:.2f} / {p99:.2f} ms")
    else:
        print("Latency: no successful requests recorded")

    print(f"\nAll output for this run: {run_dir}")


def parse_args() -> WorkloadConfig:
    defaults = WorkloadConfig()
    parser = argparse.ArgumentParser(description=__doc__)
    for field, default in asdict(defaults).items():
        flag = "--" + field.replace("_", "-")
        if isinstance(default, bool):
            # The bool fields (enable_burst, keep_alive, dry_run) default
            # to False, so store_true is enough — revisit this if a bool
            # defaulting to True ever gets added.
            parser.add_argument(flag, action="store_true")
        else:
            parser.add_argument(flag, type=type(default), default=default)
    args = parser.parse_args()
    return WorkloadConfig(**vars(args))


if __name__ == "__main__":
    cfg = parse_args()
    if cfg.dry_run:
        dry_run(cfg)
    else:
        run_workload(cfg)
