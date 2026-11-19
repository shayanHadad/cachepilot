"""
Offline comparison of cache policies on independent seeds.

For every evaluation seed (stationary and bursty) it builds the request
plan, computes the window features the way data-pipeline/build_dataset.py
does, asks the real model what it would admit (inference.decide, one
request at a time, like the live service), and replays the sequence
through LRU, LFU, a tuned frequency rule and the model on top of LRU.
It prints mean and sd per policy and the paired differences.

Setup, once, with the interpreter that has pymongo:
    /usr/bin/python3 evaluation/fetch_media_sizes.py

Run, with the ml-service venv:
    python evaluation/offline_compare.py --out /tmp/exp-03.json

Time is simulated as index / requests_per_sec (millisecond resolution, like
the request log), so there is no timing jitter and no TTL, timeout or
fallback. The frequency rule's threshold k is tuned on the training run's
plan (seed 142), never on an evaluated sequence.
"""

import argparse
import json
import subprocess
import sys
import time
from bisect import bisect_right
from collections import OrderedDict
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Sequence

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
for _sub in ("workload-generator", "ml-service"):
    _path = str(ROOT / _sub)
    if _path not in sys.path:
        sys.path.insert(0, _path)

from config import WorkloadConfig  # noqa: E402
from model.admission_sim import SimResult, simulate_lru  # noqa: E402
from workload_plan import build_plan  # noqa: E402

ONE_MIN_SEC = 60
FIVE_MIN_SEC = 300

CAPACITY = 250
REQUESTS = 60_000
REQUESTS_PER_SEC = 100.0
ZIPF = 0.9
BURST_SHARE = 0.10
EVAL_SEEDS = tuple(range(1001, 1009))
MIN_EVAL_SEED = 1000
WARMUP = 2_500

# The rule is tuned like train.py tunes it: replay the train period (first
# 80%) and keep the k with the best hit rate on its last 15%.
TUNE_SEED = 142
TRAIN_FRACTION = 0.80
VALID_FRACTION = 0.15
RULE_KS = tuple(range(1, 13))

POLICIES = ("lru", "lfu", "rule", "model")
BASELINES = ("lru", "lfu", "rule")


@dataclass(frozen=True)
class Posts:
    """Post ids in generator order with the fields the model sees."""

    ids: list[str]
    media_size_kb: list[float]
    query_types: list[str]
    ids_sha256: str


@dataclass(frozen=True)
class Features:
    frequency_1min: np.ndarray
    frequency_5min: np.ndarray
    recency_sec: np.ndarray
    inter_arrival_avg: np.ndarray


def load_posts(path: Path) -> Posts:
    """Reads the file written by fetch_media_sizes.py."""
    data = json.loads(path.read_text(encoding="utf-8"))
    sizes = [float(s) for s in data["media_size_kb"]]
    if len(sizes) != len(data["ids"]):
        raise ValueError(f"{path}: ids and media_size_kb differ in length")
    return Posts(
        ids=list(data["ids"]),
        media_size_kb=sizes,
        query_types=["text_post" if s == 0 else "media_post" for s in sizes],
        ids_sha256=str(data["post_ids_sha256"]),
    )


def compute_features(keys: np.ndarray, requests_per_sec: float,
                     timestamps_ms: Sequence[int] | None = None) -> Features:
    """Window features for every request, from the key's history before it.

    Mirrors build_dataset.compute_stats (and so tracker.go): entries older
    than 5 minutes are dropped, frequency_1min counts entries newer than
    one minute, inter_arrival_avg is the mean gap inside the window.

    Time is simulated from the request index unless `timestamps_ms` gives
    the millisecond timestamp of every request (for example from a real log).
    """
    n = len(keys)
    f1 = np.zeros(n, dtype=np.int64)
    f5 = np.zeros(n, dtype=np.int64)
    recency = np.zeros(n, dtype=np.float64)
    inter = np.zeros(n, dtype=np.float64)

    history: dict[int, list[float]] = {}
    first: dict[int, int] = {}  # index of the oldest entry still in the window
    for i in range(n):
        key = int(keys[i])
        ms = int(i * 1000.0 / requests_per_sec) if timestamps_ms is None else int(timestamps_ms[i])
        now = ms / 1000.0  # ms resolution, like the log

        ts = history.get(key)
        if ts is None:
            ts = history[key] = []
            first[key] = 0
        lo = first[key]
        cutoff = now - FIVE_MIN_SEC
        while lo < len(ts) and ts[lo] < cutoff:
            lo += 1
        first[key] = lo

        count = len(ts) - lo
        if count:
            f5[i] = count
            f1[i] = len(ts) - bisect_right(ts, now - ONE_MIN_SEC, lo)
            recency[i] = now - ts[-1]
            if count >= 2:
                diffs = [ts[j] - ts[j - 1] for j in range(lo + 1, len(ts))]
                inter[i] = sum(diffs) / len(diffs)
        ts.append(now)

    return Features(f1, f5, recency, inter)


def simulate_lfu(keys: Sequence, capacity: int, count_from: int = 0) -> SimResult:
    """LFU with go-cache-service's semantics: frequency counts only while a
    key is cached, new keys start at 1, ties evict the least recently used.
    A miss on a full cache evicts first, then inserts the new key."""
    if capacity < 1:
        raise ValueError(f"capacity must be >= 1, got {capacity}")

    freq: dict = {}
    buckets: dict[int, OrderedDict] = {}  # frequency -> keys, oldest first
    min_freq = 0
    hits = misses = counted = 0
    for i, key in enumerate(keys):
        f = freq.get(key)
        if f is not None:
            hit = True
            bucket = buckets[f]
            del bucket[key]
            if not bucket:
                del buckets[f]
                if min_freq == f:
                    min_freq = f + 1
            freq[key] = f + 1
            buckets.setdefault(f + 1, OrderedDict())[key] = None
        else:
            hit = False
            if len(freq) >= capacity:
                bucket = buckets[min_freq]
                victim, _ = bucket.popitem(last=False)
                if not bucket:
                    del buckets[min_freq]
                del freq[victim]
            freq[key] = 1
            buckets.setdefault(1, OrderedDict())[key] = None
            min_freq = 1
        if i >= count_from:
            counted += 1
            if hit:
                hits += 1
            else:
                misses += 1

    return SimResult(
        hit_rate=hits / counted if counted else 0.0,
        admit_rate=1.0 if misses else 0.0,
        requests=counted,
    )


def tune_rule_k(keys: list[int], frequency_1min: np.ndarray, capacity: int,
                ks: Sequence[int] = RULE_KS) -> tuple[int, dict[int, float]]:
    """Picks k for the rule "admit when frequency_1min >= k" by simulated
    hit rate on the validation tail of the train period. Ties go to the
    lowest k. Also returns every k's hit rate."""
    split = int(len(keys) * TRAIN_FRACTION)
    count_from = int(split * (1 - VALID_FRACTION))
    rates: dict[int, float] = {}
    best: int | None = None
    for k in ks:
        result = simulate_lru(keys[:split], (frequency_1min[:split] >= k).tolist(), capacity, count_from)
        rates[k] = result.hit_rate
        if best is None or result.hit_rate > rates[best]:
            best = k
    if best is None:
        raise ValueError("no k values given")
    return best, rates


def model_admissions(keys: list[int], feats: Features, posts: Posts, decide: Callable) -> list[bool]:
    """What the model would do on a miss at every request, scored one
    request at a time through inference.decide."""
    f1 = feats.frequency_1min.tolist()
    f5 = feats.frequency_5min.tolist()
    rec = feats.recency_sec.tolist()
    inter = feats.inter_arrival_avg.tolist()
    out = []
    for i, key in enumerate(keys):
        d = decide(posts.ids[key], f1[i], f5[i], rec[i], inter[i],
                   posts.media_size_kb[key], posts.query_types[key])
        out.append(bool(d.admit))
    return out


def evaluate_plan(keys: np.ndarray, feats: Features, posts: Posts, decide: Callable,
                  k: int, capacity: int, warmup: int) -> dict[str, dict[str, float]]:
    """Hit rates of all four policies on one sequence, counted after
    `warmup` requests (hit_rate) and over every request (hit_rate_all)."""
    key_list = keys.tolist()
    admit_all = [True] * len(key_list)
    admit_rule = (feats.frequency_1min >= k).tolist()
    admit_model = model_admissions(key_list, feats, posts, decide)

    sims: dict[str, Callable[[int], SimResult]] = {
        "lru": lambda cf: simulate_lru(key_list, admit_all, capacity, cf),
        "lfu": lambda cf: simulate_lfu(key_list, capacity, cf),
        "rule": lambda cf: simulate_lru(key_list, admit_rule, capacity, cf),
        "model": lambda cf: simulate_lru(key_list, admit_model, capacity, cf),
    }
    out = {}
    for name, sim in sims.items():
        counted, everything = sim(warmup), sim(0)
        out[name] = {
            "hit_rate": counted.hit_rate,
            "hit_rate_all": everything.hit_rate,
            "admit_rate": counted.admit_rate,
        }
    return out


def summarize(rows: list[dict], metric: str = "hit_rate") -> dict:
    """Mean and sd per policy over seeds, and the model's paired difference
    to every baseline (mean, sd, and the number of seeds it wins)."""
    values = {p: np.array([r["policies"][p][metric] for r in rows]) for p in POLICIES}

    def sd(a: np.ndarray) -> float:
        return float(a.std(ddof=1)) if len(a) > 1 else 0.0

    paired = {}
    for other in BASELINES:
        diff = values["model"] - values[other]
        paired[other] = {"mean": float(diff.mean()), "sd": sd(diff), "wins": int((diff > 0).sum())}
    return {
        "n": len(rows),
        "mean": {p: float(values[p].mean()) for p in POLICIES},
        "sd": {p: sd(values[p]) for p in POLICIES},
        "paired": paired,
    }


def format_summary(title: str, s: dict) -> str:
    lines = [title, f"  {'policy':<8}{'mean %':>9}{'sd':>7}"]
    for p in POLICIES:
        lines.append(f"  {p:<8}{100 * s['mean'][p]:9.2f}{100 * s['sd'][p]:7.2f}")
    lines.append(f"  {'model minus':<12}{'mean pts':>9}{'sd':>7}{'wins':>7}")
    for other in BASELINES:
        d = s["paired"][other]
        lines.append(f"  {other:<12}{100 * d['mean']:9.2f}{100 * d['sd']:7.2f}{d['wins']:>4}/{s['n']}")
    return "\n".join(lines)


def validate_seeds(seeds: Sequence[int]) -> None:
    low = [s for s in seeds if s < MIN_EVAL_SEED]
    if low:
        raise ValueError(f"evaluation seeds must be >= {MIN_EVAL_SEED} (training uses 42 and 142), got {low}")


def make_config(seed: int, burst: bool, args: argparse.Namespace) -> WorkloadConfig:
    return replace(
        WorkloadConfig(),
        seed=seed,
        total_requests=args.requests,
        requests_per_sec=args.rps,
        zipf_param=args.zipf,
        enable_burst=burst,
        burst_share=args.burst_share,
    )


def git_state() -> str:
    """HEAD hash, with +dirty if the working tree has any change."""
    try:
        run = lambda *a: subprocess.run(["git", "-C", str(ROOT), *a], capture_output=True, text=True, check=True).stdout.strip()
        return run("rev-parse", "HEAD") + ("+dirty" if run("status", "--porcelain") else "")
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--media", type=Path, default=ROOT / "data" / "eval" / "media_sizes.json")
    p.add_argument("--model", type=Path, default=ROOT / "ml-service" / "model" / "artifacts" / "model.txt")
    p.add_argument("--seeds", type=int, nargs="+", default=list(EVAL_SEEDS))
    p.add_argument("--requests", type=int, default=REQUESTS)
    p.add_argument("--rps", type=float, default=REQUESTS_PER_SEC)
    p.add_argument("--zipf", type=float, default=ZIPF)
    p.add_argument("--burst-share", type=float, default=BURST_SHARE)
    p.add_argument("--capacity", type=int, default=CAPACITY)
    p.add_argument("--warmup", type=int, default=WARMUP)
    p.add_argument("--tune-seed", type=int, default=TUNE_SEED)
    p.add_argument("--out", type=Path, default=None, help="write all results as JSON")
    return p.parse_args(argv)


def pct(policies: dict, metric: str = "hit_rate") -> str:
    return "  ".join(f"{p} {100 * policies[p][metric]:5.2f}" for p in POLICIES)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    validate_seeds(args.seeds)

    from model import inference  # needs lightgbm, so imported here

    posts = load_posts(args.media)
    meta_path = args.model.with_name(args.model.stem + "_meta.json")
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    if meta.get("capacity") != args.capacity:
        raise SystemExit(f"the model was trained for capacity {meta.get('capacity')}, not {args.capacity}")
    inference.init("model", args.model)

    print(f"code_version {git_state()}")
    print(f"model {meta['model_name']} threshold {meta['admit_probability_threshold']}")
    print(f"posts {len(posts.ids)} post_ids_sha256 {posts.ids_sha256}")
    print(f"seeds {args.seeds} requests {args.requests} rps {args.rps} zipf {args.zipf} "
          f"burst_share {args.burst_share} capacity {args.capacity} warmup {args.warmup}")

    started = time.monotonic()
    n_posts = len(posts.ids)

    # Tuning and reproduction check on the training run's plan.
    tune_plan = build_plan(n_posts, make_config(args.tune_seed, True, args))
    tune_feats = compute_features(tune_plan.keys, args.rps)
    k, k_rates = tune_rule_k(tune_plan.keys.tolist(), tune_feats.frequency_1min, args.capacity)
    print(f"\nrule tuned on seed {args.tune_seed} (train period, validation tail): k={k}")
    print("  hit rate by k: " + "  ".join(f"{kk}:{100 * r:.2f}" for kk, r in k_rates.items()))
    split = int(args.requests * TRAIN_FRACTION)
    reference = evaluate_plan(tune_plan.keys, tune_feats, posts, inference.decide, k, args.capacity, split)
    print(f"reproduction check, seed {args.tune_seed} last {args.requests - split} requests (model never saw them):")
    print("  " + pct(reference))

    results: dict[str, dict] = {}
    for burst in (False, True):
        name = "burst" if burst else "stationary"
        rows = []
        print(f"\n[{name}]")
        for seed in args.seeds:
            plan = build_plan(n_posts, make_config(seed, burst, args))
            feats = compute_features(plan.keys, args.rps)
            policies = evaluate_plan(plan.keys, feats, posts, inference.decide, k, args.capacity, args.warmup)
            rows.append({
                "seed": seed,
                "plan_sha256": plan.sha256,
                "bursts": len(plan.bursts),
                "burst_requests": plan.burst_requests,
                "policies": policies,
            })
            print(f"  seed {seed}  {pct(policies)}   model admit {100 * policies['model']['admit_rate']:.2f}% of misses"
                  f"  bursts {len(plan.bursts)}  [{time.monotonic() - started:.0f}s]")
        results[name] = {
            "seeds": rows,
            "after_warmup": summarize(rows, "hit_rate"),
            "all_requests": summarize(rows, "hit_rate_all"),
        }

    for name, res in results.items():
        print()
        print(format_summary(f"{name}: hit rate after warm-up ({args.warmup} requests excluded)", res["after_warmup"]))
        print(format_summary(f"{name}: hit rate over all requests", res["all_requests"]))

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps({
            "code_version": git_state(),
            "params": {k_: (str(v) if isinstance(v, Path) else v) for k_, v in vars(args).items()},
            "model": {"name": meta["model_name"], "sha256": meta["model_sha256"],
                      "threshold": meta["admit_probability_threshold"]},
            "post_ids_sha256": posts.ids_sha256,
            "n_posts": n_posts,
            "rule_k": k,
            "rule_k_validation_hit_rates": {str(kk): r for kk, r in k_rates.items()},
            "reference_seed": {"seed": args.tune_seed, "policies": reference},
            "results": results,
        }, indent=2), encoding="utf-8")
        print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
