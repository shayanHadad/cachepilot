"""
Burst-specific metrics for the offline comparison.

A burst is a cold post that suddenly goes viral. The planned bursts are
the ground truth: every burst has a window (start_index to end_index) and
every request that went to the burst key because of it carries the burst's
id. From a policy's per-request hit/miss sequence this module computes:

  hit_rate_in_window / hit_rate_outside_window
      all requests inside / outside any burst window
  hit_rate_burst
      the requests that went to a burst key because of its burst
  hit_rate_nonburst_in_window
      the other requests inside burst windows (collateral effect)
  misses_before_first_hit
      burst requests missed before the burst key is first served from
      the cache (per burst, then averaged); a burst whose key is never
      cached counts all its requests
  never_cached_share
      share of bursts whose key never hit

The module is numpy-only and imports nothing from the other tools.
"""

from collections import OrderedDict
from typing import Sequence

import numpy as np

# name -> (label, is_rate, higher_is_better)
METRICS = {
    "hit_rate_in_window": ("all requests inside burst windows", True, True),
    "hit_rate_outside_window": ("all requests outside burst windows", True, True),
    "hit_rate_burst": ("burst requests", True, True),
    "hit_rate_nonburst_in_window": ("other requests inside burst windows", True, True),
    "misses_before_first_hit": ("burst requests missed before the first hit", False, False),
    "never_cached_share": ("bursts whose key was never cached", True, False),
}

# Decision bar for the burst hypothesis, fixed before the run (fractions of hit rate).
MIN_ADVANTAGE = 0.003


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


def burst_metrics(burst_ids: np.ndarray, bursts: Sequence, hits: Sequence[bool], warmup: int) -> dict:
    """Burst metrics of one policy on one plan. `bursts` need burst_id,
    start_index and end_index. Requests before `warmup` are not counted, and
    bursts that start before it are left out of the per-burst metrics."""
    h = np.asarray(hits, dtype=bool)
    burst_ids = np.asarray(burst_ids)
    n = len(h)
    counted = np.arange(n) >= warmup
    is_burst = burst_ids >= 0
    in_window = np.zeros(n, dtype=bool)
    for b in bursts:
        in_window[b.start_index:b.end_index + 1] = True

    def rate(mask: np.ndarray) -> float | None:
        c = int(mask.sum())
        return float(h[mask].sum()) / c if c else None

    out: dict = {
        "hit_rate_in_window": rate(counted & in_window),
        "hit_rate_outside_window": rate(counted & ~in_window),
        "hit_rate_burst": rate(counted & is_burst),
        "hit_rate_nonburst_in_window": rate(counted & in_window & ~is_burst),
    }

    misses: list[int] = []
    never = 0
    for b in bursts:
        if b.start_index < warmup:
            continue
        idx = np.flatnonzero(burst_ids == b.burst_id)
        if idx.size == 0:
            continue
        first = np.flatnonzero(h[idx])
        if first.size:
            misses.append(int(first[0]))
        else:
            misses.append(int(idx.size))
            never += 1
    out["bursts_counted"] = len(misses)
    out["misses_before_first_hit"] = float(np.mean(misses)) if misses else None
    out["never_cached_share"] = never / len(misses) if misses else None
    return out


def _sd(a: np.ndarray) -> float:
    return float(a.std(ddof=1)) if len(a) > 1 else 0.0


def _paired(diff: np.ndarray, higher_is_better: bool) -> dict:
    wins = int((diff > 0).sum()) if higher_is_better else int((diff < 0).sum())
    return {"mean": float(diff.mean()), "sd": _sd(diff), "wins": wins}


def summarize_bursts(rows: Sequence[dict], policies: Sequence[str], baselines: Sequence[str],
                     model: str = "model") -> dict:
    """Mean and sd over seeds of every burst metric and policy, the model's
    paired difference to each baseline, and the model's burst advantage:
    (model minus baseline) inside burst windows minus the same outside them.
    Rows are {"burst_metrics": {policy: burst_metrics(...)}}; seeds where a
    value is missing for any policy are skipped for that metric."""
    summary: dict = {"n": len(rows), "metrics": {}, "advantage": {}}
    for metric, (_, _, higher) in METRICS.items():
        valid = [i for i, r in enumerate(rows)
                 if all(r["burst_metrics"][p][metric] is not None for p in policies)]
        if not valid:
            continue
        vals = {p: np.array([rows[i]["burst_metrics"][p][metric] for i in valid]) for p in policies}
        summary["metrics"][metric] = {
            "n": len(valid),
            "mean": {p: float(vals[p].mean()) for p in policies},
            "sd": {p: _sd(vals[p]) for p in policies},
            "paired": {b: _paired(vals[model] - vals[b], higher) for b in baselines},
        }

    both = [i for i, r in enumerate(rows)
            if all(r["burst_metrics"][p][m] is not None
                   for p in policies for m in ("hit_rate_in_window", "hit_rate_outside_window"))]
    if both:
        def column(p: str, m: str) -> np.ndarray:
            return np.array([rows[i]["burst_metrics"][p][m] for i in both])

        for b in baselines:
            gap_in = column(model, "hit_rate_in_window") - column(b, "hit_rate_in_window")
            gap_out = column(model, "hit_rate_outside_window") - column(b, "hit_rate_outside_window")
            adv = gap_in - gap_out
            summary["advantage"][b] = {
                "n": len(both), "gap_in": float(gap_in.mean()), "gap_out": float(gap_out.mean()),
                **_paired(adv, True),
            }
    return summary


def verdict(summary: dict, baseline: str = "rule10", min_advantage: float = MIN_ADVANTAGE) -> dict | None:
    """The bar fixed before the run: the model's burst advantage over the
    strongest simple rule is at least 0.3 points and positive in all but at
    most one seed."""
    adv = summary["advantage"].get(baseline)
    if adv is None:
        return None
    checks = {
        f"burst advantage over {baseline} >= {100 * min_advantage:.1f} points": adv["mean"] >= min_advantage,
        f"positive in at least {adv['n'] - 1} of {adv['n']} seeds": adv["wins"] >= adv["n"] - 1,
    }
    return {"baseline": baseline, "advantage": adv["mean"], "wins": adv["wins"], "n": adv["n"],
            "checks": checks, "passes": all(checks.values())}


def format_bursts(summary: dict, policies: Sequence[str], baselines: Sequence[str]) -> str:
    lines = []
    for metric, data in summary["metrics"].items():
        label, is_rate, higher = METRICS[metric]
        scale, unit = (100.0, "%") if is_rate else (1.0, "")
        better = "higher is better" if higher else "lower is better"
        lines.append(f"{label} ({better}; {data['n']} seeds)")
        lines.append(f"  {'policy':<8}{'mean' + unit:>9}{'sd':>7}")
        for p in policies:
            lines.append(f"  {p:<8}{scale * data['mean'][p]:9.2f}{scale * data['sd'][p]:7.2f}")
        pts = "pts" if is_rate else "count"
        lines.append(f"  {'model minus':<12}{'mean ' + pts:>9}{'sd':>7}{'wins':>7}")
        for b in baselines:
            d = data["paired"][b]
            lines.append(f"  {b:<12}{scale * d['mean']:9.2f}{scale * d['sd']:7.2f}{d['wins']:>4}/{data['n']}")
        lines.append("")
    if summary["advantage"]:
        lines.append("model burst advantage: (model minus baseline) inside burst windows minus outside them, in points")
        lines.append(f"  {'baseline':<10}{'inside':>8}{'outside':>9}{'advantage':>11}{'sd':>7}{'positive':>10}")
        for b in baselines:
            a = summary["advantage"][b]
            lines.append(f"  {b:<10}{100 * a['gap_in']:8.2f}{100 * a['gap_out']:9.2f}{100 * a['mean']:11.2f}"
                         f"{100 * a['sd']:7.2f}{a['wins']:>6}/{a['n']}")
    return "\n".join(lines)
