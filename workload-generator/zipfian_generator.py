"""
Generates a request workload against the go-cache-service, following
a Zipfian access distribution over real post ids pulled from MongoDB,
with an optional toggleable "burst" mode where a few posts
temporarily become much hotter than their normal Zipfian weight would
suggest (see docs/architecture.md for why this is a flag, not
always-on).

Usage:
    pip install pymongo numpy requests --break-system-packages
    python zipfian_generator.py --total-requests 5000 --requests-per-sec 50
    python zipfian_generator.py --enable-burst --total-requests 5000
"""

import argparse
import json
import time
from dataclasses import asdict
from pathlib import Path

import numpy as np
import requests
from pymongo import MongoClient

from config import WorkloadConfig


def load_post_ids(mongo_uri: str, db_name: str) -> list[str]:
    """Pulls every post _id from MongoDB, as hex strings — these are
    the only valid keys the service can actually serve, so the
    workload has to be built from real data, not made-up ids."""
    client = MongoClient(mongo_uri)
    ids = [str(doc["_id"]) for doc in client[db_name]["posts"].find({}, {"_id": 1})]
    if not ids:
        raise RuntimeError(
            f"No posts found in {db_name}.posts — run scripts/seed_posts.py first."
        )
    return ids


def zipfian_weights(n: int, s: float) -> np.ndarray:
    """Builds a normalized probability array of length n following
    Zipf's law: rank 1 is the most popular, weight ~ 1/rank^s.

    Using numpy.random.zipf directly isn't a good fit here because
    its support is unbounded (it can return ranks far larger than the
    number of posts that actually exist) — building our own bounded,
    normalized array and sampling with np.random.choice keeps every
    sampled rank mapped to a real post id.
    """
    ranks = np.arange(1, n + 1)
    weights = 1.0 / np.power(ranks, s)
    return weights / weights.sum()


class BurstState:
    """Tracks which post indices are currently "bursting" and
    recomputes sampling weights only when that set actually changes,
    rather than on every single request — recomputing a weights array
    of size n on every request would get expensive for large post
    counts at high request rates.

    Burst candidates are restricted to posts that have been idle for
    at least burst_min_idle_sec (see maybe_start_burst). Earlier this
    picked any random post, which meant a "burst" could land on a
    post that was already naturally hot — that has basically no
    effect, since a hot post wasn't at risk of eviction to begin with.
    Requiring idle time first is what actually models the scenario
    this project cares about: a previously cold post suddenly going
    viral.
    """

    def __init__(self, base_weights: np.ndarray, cfg: WorkloadConfig, rng: np.random.Generator):
        self.base_weights = base_weights
        self.cfg = cfg
        self.rng = rng
        self.active: dict[int, float] = {}  # post index -> end_time (unix seconds)
        self._weights = base_weights
        self._dirty = False
        # -inf means "never requested yet", which is trivially >=
        # burst_min_idle_sec idle — a post that hasn't been touched
        # at all is at least as eligible as one that's merely been
        # quiet for a while.
        self.last_access = np.full(base_weights.shape[0], -np.inf)

    def record_access(self, idx: int, now: float) -> None:
        """Call this every time a key is actually requested, so idle
        time is tracked for every post, not just ones that have
        bursted before."""
        self.last_access[idx] = now

    def maybe_start_burst(self, now: float, n: int) -> int | None:
        """With probability cfg.burst_rate per second, start a new
        burst on a random post that (a) isn't already bursting and
        (b) has been idle for at least cfg.burst_min_idle_sec —
        modeling a previously cold post suddenly going viral, not an
        already-hot post getting even hotter. Returns the started
        post's index, or None if no burst started (including the case
        where no idle-enough candidate currently exists)."""
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
        """Returns the current sampling weights, recomputing them
        only if the active burst set changed since the last call."""
        if self._dirty:
            w = self.base_weights.copy()
            for idx in self.active:
                w[idx] *= self.cfg.burst_intensity
            self._weights = w / w.sum()
            self._dirty = False
        return self._weights


def run_workload(cfg: WorkloadConfig) -> None:
    rng = np.random.default_rng(cfg.seed)

    post_ids = load_post_ids(cfg.mongo_uri, cfg.mongo_db)
    n = len(post_ids)
    print(f"Loaded {n} post ids from {cfg.mongo_db}.posts")

    base_weights = zipfian_weights(n, cfg.zipf_param)
    burst = BurstState(base_weights, cfg, rng)

    Path(cfg.results_path).parent.mkdir(parents=True, exist_ok=True)
    results_file = open(cfg.results_path, "a", encoding="utf-8")

    burst_gt_file = None
    if cfg.enable_burst:
        Path(cfg.burst_ground_truth_path).parent.mkdir(parents=True, exist_ok=True)
        burst_gt_file = open(cfg.burst_ground_truth_path, "a", encoding="utf-8")

    successes = 0
    failures = 0
    latencies_ms: list[float] = []

    start_time = time.monotonic()
    print(f"Starting workload: {cfg.total_requests} requests at {cfg.requests_per_sec}/s "
          f"(burst={'on' if cfg.enable_burst else 'off'})")

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

        # Pace requests to the target rate by sleeping until the
        # scheduled time for the *next* request, rather than a fixed
        # sleep(1/rate) after each one — the fixed-sleep approach
        # accumulates drift over many iterations because it ignores
        # how long the request itself took.
        next_request_at = start_time + (i + 1) / cfg.requests_per_sec
        sleep_for = next_request_at - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)

    results_file.close()
    if burst_gt_file is not None:
        burst_gt_file.close()

    elapsed = time.monotonic() - start_time
    avg_latency = sum(latencies_ms) / len(latencies_ms) if latencies_ms else 0.0
    print("\n--- Workload summary ---")
    print(f"Total requests:   {cfg.total_requests}")
    print(f"Successes:        {successes}")
    print(f"Failures:         {failures}")
    print(f"Elapsed:          {elapsed:.1f}s")
    print(f"Avg latency:      {avg_latency:.2f}ms")
    print(f"Results written:  {cfg.results_path}")
    if cfg.enable_burst:
        print(f"Burst ground truth: {cfg.burst_ground_truth_path}")


def parse_args() -> WorkloadConfig:
    defaults = WorkloadConfig()
    parser = argparse.ArgumentParser(description=__doc__)
    for field, default in asdict(defaults).items():
        flag = "--" + field.replace("_", "-")
        if isinstance(default, bool):
            # Only enable_burst is boolean today, and it defaults to
            # False, so a simple store_true flag is enough — this
            # would need to be more general if a bool field defaulting
            # to True is ever added.
            parser.add_argument(flag, action="store_true")
        else:
            parser.add_argument(flag, type=type(default), default=default)
    args = parser.parse_args()
    return WorkloadConfig(**vars(args))


if __name__ == "__main__":
    run_workload(parse_args())