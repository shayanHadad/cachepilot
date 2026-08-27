"""
Sends a Zipfian-distributed request workload at go-cache-service,
using real post ids from MongoDB. Burst mode is optional — when on, a
few idle posts randomly spike in popularity for a while (see
docs/architecture.md for why this is a toggle, not always-on).

Each run gets its own timestamped folder with results, config, and a
snapshot of the Go service's log — see create_run_dir.

Usage:
    pip install pymongo numpy requests --break-system-packages
    python zipfian_generator.py --total-requests 5000 --requests-per-sec 50
    python zipfian_generator.py --enable-burst --total-requests 5000
"""

import argparse
import json
import shutil
import time
from dataclasses import asdict
from pathlib import Path

import numpy as np
import requests
from pymongo import MongoClient

from config import WorkloadConfig


def load_post_ids(mongo_uri: str, db_name: str) -> list[str]:
    """All post ids in Mongo — the only valid keys the service can serve."""
    client = MongoClient(mongo_uri)
    ids = [str(doc["_id"]) for doc in client[db_name]["posts"].find({}, {"_id": 1})]
    if not ids:
        raise RuntimeError(
            f"No posts found in {db_name}.posts — run scripts/seed_posts.py first."
        )
    return ids


def zipfian_weights(n: int, s: float) -> np.ndarray:
    """Normalized probability array of length n: rank 1 is most
    popular, weight ~ 1/rank^s.

    Not using numpy.random.zipf directly because its range is
    unbounded — it could hand back a rank higher than we actually
    have posts for. Building our own bounded array keeps every
    sampled rank mapped to a real post.
    """
    ranks = np.arange(1, n + 1)
    weights = 1.0 / np.power(ranks, s)
    return weights / weights.sum()


class BurstState:
    """Tracks which posts are currently bursting.

    Candidates for a new burst have to be idle for a while first
    (see maybe_start_burst) — picking any random post used to mean a
    "burst" could land on something already hot, which barely
    changes anything since a hot post wasn't at risk of eviction
    anyway. Idle-first is what actually models a cold post suddenly
    going viral.
    """

    def __init__(self, base_weights: np.ndarray, cfg: WorkloadConfig, rng: np.random.Generator):
        self.base_weights = base_weights
        self.cfg = cfg
        self.rng = rng
        self.active: dict[int, float] = {}  # post index -> burst end time
        self._weights = base_weights
        self._dirty = False
        # -inf = never requested, which counts as idle from the start.
        self.last_access = np.full(base_weights.shape[0], -np.inf)

    def record_access(self, idx: int, now: float) -> None:
        self.last_access[idx] = now

    def maybe_start_burst(self, now: float, n: int) -> int | None:
        """Rolls the dice for a new burst; if one starts, picks an
        idle-enough post and returns its index."""
        p_per_request = self.cfg.burst_rate / max(self.cfg.requests_per_sec, 1e-9)
        if self.rng.random() >= p_per_request:
            return None

        idle_for = now - self.last_access
        eligible = idle_for >= self.cfg.burst_min_idle_sec
        candidates = [i for i in range(n) if eligible[i] and i not in self.active]
        if not candidates:
            return None

        idx = int(self.rng.choice(candidates))
        self.active[idx] = now + self.cfg.burst_duration_sec
        self._dirty = True
        return idx

    def expire_old_bursts(self, now: float) -> None:
        expired = [idx for idx, end in self.active.items() if end <= now]
        for idx in expired:
            del self.active[idx]
        if expired:
            self._dirty = True

    def weights(self) -> np.ndarray:
        """Current sampling weights, recomputed only when the active
        burst set actually changed."""
        if self._dirty:
            w = self.base_weights.copy()
            for idx in self.active:
                w[idx] *= self.cfg.burst_intensity
            self._weights = w / w.sum()
            self._dirty = False
        return self._weights


def create_run_dir(cfg: WorkloadConfig) -> Path:
    """Makes a fresh, uniquely named output folder for this run
    (timestamp + burst on/off), so runs never overwrite each other —
    no more manually mv-ing files between runs."""
    timestamp = time.strftime("%Y-%m-%d_%H%M%S")
    tag = "burst" if cfg.enable_burst else "normal"
    run_dir = Path(cfg.output_root) / f"run_{timestamp}_{tag}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def write_run_metadata(run_dir: Path, cfg: WorkloadConfig) -> None:
    """Dumps every parameter for this run to run_config.json, so
    nobody has to remember what flags produced a given result."""
    metadata = asdict(cfg)
    metadata["run_started_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    with open(run_dir / "run_config.json", "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)


def run_workload(cfg: WorkloadConfig) -> None:
    rng = np.random.default_rng(cfg.seed)

    post_ids = load_post_ids(cfg.mongo_uri, cfg.mongo_db)
    n = len(post_ids)
    print(f"Loaded {n} post ids from {cfg.mongo_db}.posts")

    base_weights = zipfian_weights(n, cfg.zipf_param)
    burst = BurstState(base_weights, cfg, rng)

    run_dir = create_run_dir(cfg)
    write_run_metadata(run_dir, cfg)
    print(f"Run output directory: {run_dir}")

    results_file = open(run_dir / "results.jsonl", "a", encoding="utf-8")

    burst_gt_file = None
    if cfg.enable_burst:
        burst_gt_file = open(run_dir / "burst_ground_truth.jsonl", "a", encoding="utf-8")

    successes = 0
    failures = 0
    latencies_ms: list[float] = []

    start_time = time.monotonic()
    print(f"Starting workload: {cfg.total_requests} requests at {cfg.requests_per_sec}/s "
          f"(burst={'on' if cfg.enable_burst else 'off'})")

    progress_every = max(cfg.total_requests // 10, 1)

    for i in range(cfg.total_requests):
        now = time.monotonic()

        if cfg.enable_burst:
            burst.expire_old_bursts(now)
            started_idx = burst.maybe_start_burst(now, n)
            if started_idx is not None and burst_gt_file is not None:
                burst_gt_file.write(json.dumps({
                    "start_ts": int(time.time() * 1000),
                    "end_ts": int((time.time() + cfg.burst_duration_sec) * 1000),
                    "key": post_ids[started_idx],
                }) + "\n")

        weights = burst.weights() if cfg.enable_burst else base_weights
        idx = rng.choice(n, p=weights)
        key = post_ids[idx]
        burst.record_access(idx, now)

        req_start = time.monotonic()
        try:
            resp = requests.get(cfg.service_url + "/get", params={"key": key}, timeout=5)
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
            "is_burst": cfg.enable_burst and idx in burst.active,
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
            # Only enable_burst is bool today and it defaults to
            # False — revisit this if a bool defaulting to True
            # ever gets added.
            parser.add_argument(flag, action="store_true")
        else:
            parser.add_argument(flag, type=type(default), default=default)
    args = parser.parse_args()
    return WorkloadConfig(**vars(args))


if __name__ == "__main__":
    run_workload(parse_args())