"""
Offline simulator for an admission rule on top of an LRU cache.

It replays a request sequence the way go-cache-service's LRU handles it:
a hit moves the key to the front, a miss inserts the key only when the
admission rule says so, and the least recently used key is evicted when
the cache holds more than `capacity` keys. There is no TTL and no timing.

train.py uses it to choose the admission threshold by hit rate on the
validation period, because a good classification score can still admit
almost nothing or almost everything.
"""

from collections import OrderedDict
from dataclasses import dataclass
from typing import Sequence

import numpy as np

# Candidate probability thresholds, 0.05 to 0.95.
DEFAULT_THRESHOLDS = tuple(round(0.05 * i, 2) for i in range(1, 20))


@dataclass(frozen=True)
class SimResult:
    hit_rate: float    # hits / counted requests
    admit_rate: float  # admitted / counted misses
    requests: int      # requests counted


def simulate_lru(keys: Sequence, admit: Sequence[bool], capacity: int, count_from: int = 0) -> SimResult:
    """Replays `keys`; admit[i] says whether a miss at request i is cached.

    The cache starts empty at request 0, but only requests from
    `count_from` on are counted, so the cache can warm up first.
    """
    if capacity < 1:
        raise ValueError(f"capacity must be >= 1, got {capacity}")
    if len(keys) != len(admit):
        raise ValueError(f"keys ({len(keys)}) and admit ({len(admit)}) differ in length")

    cache: OrderedDict = OrderedDict()
    hits = misses = admitted = counted = 0
    for i, key in enumerate(keys):
        if key in cache:
            cache.move_to_end(key)
            hit = True
        else:
            hit = False
            if admit[i]:
                cache[key] = None
                if len(cache) > capacity:
                    cache.popitem(last=False)
        if i >= count_from:
            counted += 1
            if hit:
                hits += 1
            else:
                misses += 1
                admitted += bool(admit[i])

    return SimResult(
        hit_rate=hits / counted if counted else 0.0,
        admit_rate=admitted / misses if misses else 0.0,
        requests=counted,
    )


def best_cutoff(
    keys: Sequence,
    scores: np.ndarray,
    cutoffs: Sequence[float],
    capacity: int,
    count_from: int = 0,
) -> tuple[float, SimResult]:
    """Returns the cutoff whose rule "admit when score >= cutoff" gives the
    highest hit rate, with its result. Ties go to the lowest cutoff."""
    scores = np.asarray(scores)
    best: tuple[float, SimResult] | None = None
    for cutoff in cutoffs:
        result = simulate_lru(keys, scores >= cutoff, capacity, count_from)
        if best is None or result.hit_rate > best[1].hit_rate:
            best = (float(cutoff), result)
    if best is None:
        raise ValueError("no cutoffs given")
    return best
