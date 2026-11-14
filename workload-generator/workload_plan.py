"""
Builds the request plan for one workload run: which post is requested at
every step, and which of those requests belong to a burst.

The plan is pure and deterministic — numpy only, no I/O, no wall clock.
Time is simulated as index / requests_per_sec, so the same config, seed
and post count always give the same plan (see Plan.sha256), and a plan
can be inspected without MongoDB or the service
(zipfian_generator.py --dry-run).

Stationary requests come from one random stream and bursts from a second
one. With bursts off the sequence is exactly what the generator produced
before bursts were planned, and a burst run is the stationary run with
some requests replaced by the burst key.
"""

import hashlib
from dataclasses import dataclass

import numpy as np

from config import WorkloadConfig


@dataclass(frozen=True)
class Burst:
    """One burst of traffic on a single post."""

    burst_id: int
    key_index: int    # index into the post id list; rank = key_index + 1
    start_index: int  # first request index at which the burst is active
    end_index: int    # last request index at which it is active
    requests: int     # requests that went to the burst key because of it


@dataclass(frozen=True)
class Plan:
    keys: np.ndarray       # post index for every request
    burst_ids: np.ndarray  # burst id for burst requests, -1 otherwise
    bursts: tuple[Burst, ...]
    sha256: str            # fingerprint of keys and burst_ids

    @property
    def burst_requests(self) -> int:
        return int((self.burst_ids >= 0).sum())


@dataclass
class _ActiveBurst:
    burst_id: int
    key_index: int
    start_index: int
    end_time: float
    end_index: int


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


def hash_post_ids(post_ids: list[str]) -> str:
    """Fingerprint of the ordered post id list, to notice a re-seeded database."""
    return hashlib.sha256("\n".join(post_ids).encode("utf-8")).hexdigest()


def _validate(n_posts: int, cfg: WorkloadConfig) -> None:
    if n_posts < 1:
        raise ValueError(f"need at least one post, got {n_posts}")
    if cfg.total_requests < 0:
        raise ValueError(f"total_requests must be >= 0, got {cfg.total_requests}")
    if cfg.requests_per_sec <= 0:
        raise ValueError(f"requests_per_sec must be > 0, got {cfg.requests_per_sec}")
    if not 0.0 <= cfg.burst_share <= 1.0:
        raise ValueError(f"burst_share must be between 0 and 1, got {cfg.burst_share}")
    if cfg.burst_rate < 0:
        raise ValueError(f"burst_rate must be >= 0, got {cfg.burst_rate}")
    if cfg.burst_duration_sec <= 0:
        raise ValueError(f"burst_duration_sec must be > 0, got {cfg.burst_duration_sec}")
    if cfg.burst_min_idle_sec < 0:
        raise ValueError(f"burst_min_idle_sec must be >= 0, got {cfg.burst_min_idle_sec}")


def _fingerprint(keys: np.ndarray, burst_ids: np.ndarray) -> str:
    digest = hashlib.sha256()
    digest.update(keys.astype("<i8").tobytes())
    digest.update(burst_ids.astype("<i8").tobytes())
    return digest.hexdigest()


def build_plan(n_posts: int, cfg: WorkloadConfig) -> Plan:
    """Returns the request plan for n_posts posts under cfg.

    While a burst is active, each request goes to the burst key with
    probability burst_share. Overlapping bursts take their shares
    independently, in start order. A new burst can only start on a post
    that has not been requested for burst_min_idle_sec.
    """
    _validate(n_posts, cfg)
    total = cfg.total_requests
    rps = cfg.requests_per_sec

    base_rng = np.random.default_rng(cfg.seed)
    burst_rng = np.random.default_rng([cfg.seed, 1])
    weights = zipfian_weights(n_posts, cfg.zipf_param)

    keys = np.empty(total, dtype=np.int64)
    burst_ids = np.full(total, -1, dtype=np.int64)
    last_access = np.full(n_posts, -np.inf)  # -inf = never requested = idle
    bursts: list[_ActiveBurst] = []
    active: list[_ActiveBurst] = []
    p_start = min(cfg.burst_rate / rps, 1.0)

    for i in range(total):
        t = i / rps
        # One stationary draw per request, burst or not, keeps that stream intact.
        key = int(base_rng.choice(n_posts, p=weights))

        if cfg.enable_burst:
            for burst in [b for b in active if b.end_time <= t]:
                burst.end_index = i - 1
                active.remove(burst)

            if burst_rng.random() < p_start:
                candidates = np.flatnonzero((t - last_access) >= cfg.burst_min_idle_sec)
                if active:
                    candidates = candidates[~np.isin(candidates, [b.key_index for b in active])]
                if candidates.size:
                    burst = _ActiveBurst(
                        burst_id=len(bursts),
                        key_index=int(burst_rng.choice(candidates)),
                        start_index=i,
                        end_time=t + cfg.burst_duration_sec,
                        end_index=total - 1,
                    )
                    bursts.append(burst)
                    active.append(burst)

            for burst in active:
                if burst_rng.random() < cfg.burst_share:
                    key = burst.key_index
                    burst_ids[i] = burst.burst_id
                    break

        keys[i] = key
        last_access[key] = t

    counts = np.bincount(burst_ids[burst_ids >= 0], minlength=len(bursts))
    return Plan(
        keys=keys,
        burst_ids=burst_ids,
        bursts=tuple(
            Burst(b.burst_id, b.key_index, b.start_index, b.end_index, int(counts[b.burst_id]))
            for b in bursts
        ),
        sha256=_fingerprint(keys, burst_ids),
    )
