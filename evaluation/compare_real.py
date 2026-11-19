"""
Compares a real run of go-cache-service with the offline simulation of the
same request plan.

It rebuilds the plan from the seed, checks that the keys in the service's
live log are exactly the planned keys, and then compares the logged
hit/miss sequence with the simulated one, request by request.

  lru, lfu  the algorithms are deterministic and requests are sequential,
            so the two sequences should be identical. Any difference is a
            bug in the simulator, the plan or the service.
  ml        the model decides on real timing, so small differences are
            expected. Two simulations are compared: one the way
            offline_compare.py does it (simulated time), and one that uses
            the logged timestamps and the fallbacks that really happened.

Usage, with the ML service venv (the log must come from one run with its
own empty log file; use the live log, not the generator's snapshot):
    python evaluation/compare_real.py --log data/eval_runs/lru_burst/service.jsonl \\
        --policy lru --seed 1001 --burst
"""

import argparse
import json
import sys
from collections import Counter, OrderedDict
from pathlib import Path
from typing import Sequence

import offline_compare as oc

FALLBACK_SOURCE = "fallback-lru"
HIT_SOURCE = "cache-hit"
EARLY_REQUESTS = 100  # the lazy gRPC connection makes the first calls fall back
MAX_FALLBACK_SHARE = 0.01


def read_log(path: Path) -> tuple[list[dict], int]:
    """Reads a JSONL service log in file order. Returns the records and the
    number of malformed lines that were skipped."""
    records: list[dict] = []
    skipped = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                skipped += 1
    return records, skipped


def check_keys(expected: Sequence[str], records: Sequence[dict]) -> dict:
    """Checks that the logged keys are the planned keys, in order."""
    common = min(len(expected), len(records))
    first_mismatch = next((i for i in range(common) if records[i]["key"] != expected[i]), None)
    return {
        "planned": len(expected),
        "logged": len(records),
        "first_mismatch": first_mismatch,
        "ok": first_mismatch is None and len(records) <= len(expected),
        "complete": first_mismatch is None and len(records) == len(expected),
    }


def hit_sequence_lru(keys: Sequence, admit: Sequence[bool], capacity: int) -> list[bool]:
    """Hit or miss for every request of an LRU cache with an admission rule."""
    cache: OrderedDict = OrderedDict()
    hits = []
    for key, may_admit in zip(keys, admit):
        if key in cache:
            cache.move_to_end(key)
            hits.append(True)
            continue
        hits.append(False)
        if may_admit:
            cache[key] = None
            if len(cache) > capacity:
                cache.popitem(last=False)
    return hits


def hit_sequence_lfu(keys: Sequence, capacity: int) -> list[bool]:
    """Hit or miss for every request of the Go LFU (evict first, then insert;
    ties go to the least recently used key)."""
    freq: dict = {}
    buckets: dict[int, OrderedDict] = {}
    min_freq = 0
    hits = []
    for key in keys:
        f = freq.get(key)
        if f is not None:
            hits.append(True)
            bucket = buckets[f]
            del bucket[key]
            if not bucket:
                del buckets[f]
                if min_freq == f:
                    min_freq = f + 1
            freq[key] = f + 1
            buckets.setdefault(f + 1, OrderedDict())[key] = None
            continue
        hits.append(False)
        if len(freq) >= capacity:
            bucket = buckets[min_freq]
            victim, _ = bucket.popitem(last=False)
            if not bucket:
                del buckets[min_freq]
            del freq[victim]
        freq[key] = 1
        buckets.setdefault(1, OrderedDict())[key] = None
        min_freq = 1
    return hits


def compare_hits(real: Sequence[bool], sim: Sequence[bool], warmup: int) -> dict:
    """Request-by-request comparison of two hit/miss sequences."""
    if len(real) != len(sim):
        raise ValueError(f"sequences differ in length: {len(real)} and {len(sim)}")

    def rate(hits: Sequence[bool], start: int) -> float:
        part = hits[start:]
        return sum(part) / len(part) if part else 0.0

    mismatches = [i for i in range(len(real)) if real[i] != sim[i]]
    return {
        "requests": len(real),
        "real_hit_rate": rate(real, warmup),
        "sim_hit_rate": rate(sim, warmup),
        "real_hit_rate_all": rate(real, 0),
        "sim_hit_rate_all": rate(sim, 0),
        "mismatches": len(mismatches),
        "mismatches_after_warmup": sum(1 for i in mismatches if i >= warmup),
        "first_mismatch": mismatches[0] if mismatches else None,
        "real_hit_sim_miss": sum(1 for i in mismatches if real[i]),
        "real_miss_sim_hit": sum(1 for i in mismatches if not real[i]),
    }


def source_breakdown(records: Sequence[dict], model_name: str | None) -> dict:
    """Who produced each response, and how many model-needing misses fell
    back to LRU (in total, in the first requests and afterwards)."""
    sources = Counter(r["source"] for r in records)
    misses = [i for i, r in enumerate(records) if r["cache_status"] != "hit"]
    fallbacks = [i for i in misses if records[i]["source"] == FALLBACK_SOURCE]
    late_misses = [i for i in misses if i >= EARLY_REQUESTS]
    late_fallbacks = [i for i in fallbacks if i >= EARLY_REQUESTS]
    share = len(late_fallbacks) / len(late_misses) if late_misses else 0.0
    unexpected = sorted(s for s in sources if s not in (HIT_SOURCE, FALLBACK_SOURCE, model_name))
    return {
        "sources": dict(sources),
        "misses": len(misses),
        "fallbacks": len(fallbacks),
        "fallbacks_in_first_requests": len(fallbacks) - len(late_fallbacks),
        "fallback_share_after_first_requests": share,
        "valid": share <= MAX_FALLBACK_SHARE,
        "unexpected_sources": unexpected,
    }


def format_comparison(title: str, c: dict) -> str:
    lines = [
        title,
        f"  hit rate after warm-up   real {100 * c['real_hit_rate']:6.2f}   simulated {100 * c['sim_hit_rate']:6.2f}",
        f"  hit rate, all requests   real {100 * c['real_hit_rate_all']:6.2f}   simulated {100 * c['sim_hit_rate_all']:6.2f}",
    ]
    if c["mismatches"] == 0:
        lines.append(f"  hit/miss sequences: IDENTICAL over {c['requests']} requests")
    else:
        lines.append(
            f"  hit/miss sequences differ at {c['mismatches']} of {c['requests']} requests "
            f"({100 * c['mismatches'] / c['requests']:.2f}%), first at index {c['first_mismatch']}, "
            f"{c['mismatches_after_warmup']} after warm-up"
        )
        lines.append(f"  real hit / simulated miss {c['real_hit_sim_miss']}, real miss / simulated hit {c['real_miss_sim_hit']}")
    return "\n".join(lines)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--log", type=Path, required=True, help="live service.jsonl of one run")
    p.add_argument("--policy", choices=("lru", "lfu", "ml"), required=True)
    p.add_argument("--seed", type=int, required=True)
    p.add_argument("--burst", action="store_true", help="the run used --enable-burst")
    p.add_argument("--requests", type=int, default=20_000)
    p.add_argument("--rps", type=float, default=oc.REQUESTS_PER_SEC)
    p.add_argument("--zipf", type=float, default=oc.ZIPF)
    p.add_argument("--burst-share", type=float, default=oc.BURST_SHARE)
    p.add_argument("--capacity", type=int, default=oc.CAPACITY)
    p.add_argument("--warmup", type=int, default=oc.WARMUP)
    p.add_argument("--media", type=Path, default=oc.ROOT / "data" / "eval" / "media_sizes.json")
    p.add_argument("--model", type=Path, default=oc.ROOT / "ml-service" / "model" / "artifacts" / "model.txt")
    p.add_argument("--run-config", type=Path, default=None, help="the generator's run_config.json, to check plan_sha256")
    p.add_argument("--out", type=Path, default=None, help="write the results as JSON")
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    oc.validate_seeds([args.seed])

    posts = oc.load_posts(args.media)
    records, skipped = read_log(args.log)
    plan = oc.build_plan(len(posts.ids), oc.make_config(args.seed, args.burst, args))

    print(f"code_version {oc.git_state()}")
    print(f"log {args.log}: {len(records)} records, {skipped} malformed line(s) skipped")
    print(f"policy {args.policy} seed {args.seed} burst {args.burst} requests {args.requests} "
          f"capacity {args.capacity} warmup {args.warmup}")
    print(f"plan_sha256 {plan.sha256}")
    if args.run_config:
        recorded = json.loads(args.run_config.read_text(encoding="utf-8")).get("plan_sha256")
        print(f"run_config plan_sha256 {recorded} -> {'MATCH' if recorded == plan.sha256 else 'DIFFERENT'}")
        if recorded != plan.sha256:
            print("the run did not use this plan (other flags, seed or database); stopping")
            return 2

    expected = [posts.ids[k] for k in plan.keys.tolist()]
    keycheck = check_keys(expected, records)
    print(f"logged keys vs plan: {'OK' if keycheck['ok'] else 'MISMATCH'} "
          f"({keycheck['logged']} logged, {keycheck['planned']} planned)")
    if not keycheck["ok"]:
        print(f"first mismatch at index {keycheck['first_mismatch']}. Likely causes: a failed request "
              f"(not logged), a log that was not empty at the start, or different run flags.")
        return 2
    if not keycheck["complete"]:
        print(f"warning: only the first {keycheck['logged']} planned requests are in the log; comparing those")

    n = len(records)
    keys = plan.keys.tolist()[:n]
    real = [r["cache_status"] == "hit" for r in records]
    results: dict = {"keycheck": keycheck}

    if args.policy == "lru":
        results["lru"] = compare_hits(real, hit_sequence_lru(keys, [True] * n, args.capacity), args.warmup)
        print("\n" + format_comparison("lru: real run against the simulator", results["lru"]))
        exact = results["lru"]["mismatches"] == 0
    elif args.policy == "lfu":
        results["lfu"] = compare_hits(real, hit_sequence_lfu(keys, args.capacity), args.warmup)
        print("\n" + format_comparison("lfu: real run against the simulator", results["lfu"]))
        exact = results["lfu"]["mismatches"] == 0
    else:
        from model import inference  # needs lightgbm, so imported here

        meta = json.loads(args.model.with_name(args.model.stem + "_meta.json").read_text(encoding="utf-8"))
        inference.init("model", args.model)

        breakdown = source_breakdown(records, meta["model_name"])
        results["sources"] = breakdown
        print(f"\nsources: {breakdown['sources']}")
        print(f"fallback-lru: {breakdown['fallbacks']} of {breakdown['misses']} misses "
              f"({breakdown['fallbacks_in_first_requests']} in the first {EARLY_REQUESTS} requests); "
              f"share afterwards {100 * breakdown['fallback_share_after_first_requests']:.2f}% -> "
              f"{'VALID' if breakdown['valid'] else 'INVALID'} (limit {100 * MAX_FALLBACK_SHARE:.0f}%)")
        if breakdown["unexpected_sources"]:
            print(f"unexpected sources (not the model in {args.model.name}): {breakdown['unexpected_sources']}")

        plan_keys = plan.keys[:n]
        feats_sim = oc.compute_features(plan_keys, args.rps)
        admit_sim = oc.model_admissions(keys, feats_sim, posts, inference.decide)
        results["ml_simulated_time"] = compare_hits(real, hit_sequence_lru(keys, admit_sim, args.capacity), args.warmup)
        print("\n" + format_comparison("ml: offline simulation as offline_compare.py does it (simulated time)",
                                       results["ml_simulated_time"]))

        feats_log = oc.compute_features(plan_keys, args.rps, timestamps_ms=[r["timestamp"] for r in records])
        admit_log = oc.model_admissions(keys, feats_log, posts, inference.decide)
        admit_log = [True if r["source"] == FALLBACK_SOURCE else a for r, a in zip(records, admit_log)]
        results["ml_logged_time"] = compare_hits(real, hit_sequence_lru(keys, admit_log, args.capacity), args.warmup)
        print("\n" + format_comparison("ml: simulation with the logged timestamps and the real fallbacks",
                                       results["ml_logged_time"]))
        exact = None

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps({
            "code_version": oc.git_state(),
            "params": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()},
            "plan_sha256": plan.sha256,
            "results": results,
        }, indent=2), encoding="utf-8")
        print(f"\nwrote {args.out}")
    if exact is False:
        print("\nRESULT: the real run and the simulator differ")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
