"""
Tests for workload_plan. Run from workload-generator/:

    python -m unittest -v
"""

import unittest

import numpy as np

from config import WorkloadConfig
from workload_plan import build_plan, zipfian_weights

N_POSTS = 1000

BURST = dict(
    enable_burst=True,
    burst_rate=1 / 30,
    burst_share=0.10,
    burst_duration_sec=20.0,
    burst_min_idle_sec=5.0,
)


def make_cfg(**overrides) -> WorkloadConfig:
    values = dict(zipf_param=0.9, total_requests=12000, requests_per_sec=100.0, seed=42)
    values.update(overrides)
    return WorkloadConfig(**values)


class PlanTest(unittest.TestCase):
    def test_stationary_plan_matches_the_previous_generator(self):
        # Before bursts were planned, the generator drew one
        # rng.choice(n, p=weights) per request from default_rng(seed).
        cfg = make_cfg(total_requests=2000)
        rng = np.random.default_rng(cfg.seed)
        weights = zipfian_weights(N_POSTS, cfg.zipf_param)
        previous = [int(rng.choice(N_POSTS, p=weights)) for _ in range(cfg.total_requests)]

        plan = build_plan(N_POSTS, cfg)

        self.assertEqual(plan.keys.tolist(), previous)
        self.assertEqual(plan.burst_requests, 0)

    def test_same_seed_gives_the_same_plan(self):
        cfg = make_cfg(**BURST)
        self.assertEqual(build_plan(N_POSTS, cfg).sha256, build_plan(N_POSTS, cfg).sha256)

    def test_different_seed_gives_a_different_plan(self):
        a = build_plan(N_POSTS, make_cfg(**BURST))
        b = build_plan(N_POSTS, make_cfg(seed=7, **BURST))
        self.assertNotEqual(a.sha256, b.sha256)

    def test_burst_run_only_replaces_burst_requests(self):
        stationary = build_plan(N_POSTS, make_cfg())
        bursty = build_plan(N_POSTS, make_cfg(**BURST))

        self.assertGreater(len(bursty.bursts), 0)
        differs = stationary.keys != bursty.keys
        self.assertTrue(np.all(bursty.burst_ids[differs] >= 0))

    def test_burst_requests_hit_the_burst_key_inside_its_window(self):
        plan = build_plan(N_POSTS, make_cfg(**BURST))

        for i in np.flatnonzero(plan.burst_ids >= 0):
            burst = plan.bursts[plan.burst_ids[i]]
            self.assertEqual(plan.keys[i], burst.key_index)
            self.assertTrue(burst.start_index <= i <= burst.end_index)

    def test_burst_size_follows_the_share(self):
        cfg = make_cfg(**BURST)
        plan = build_plan(N_POSTS, cfg)
        target = cfg.burst_share * cfg.burst_duration_sec * cfg.requests_per_sec

        self.assertGreater(len(plan.bursts), 1)
        mean = sum(b.requests for b in plan.bursts) / len(plan.bursts)
        self.assertGreater(mean, 0.7 * target)
        self.assertLess(mean, 1.3 * target)

    def test_burst_key_was_idle_before_the_burst_started(self):
        cfg = make_cfg(**BURST)
        plan = build_plan(N_POSTS, cfg)
        rps = cfg.requests_per_sec

        for burst in plan.bursts:
            earlier = np.flatnonzero(plan.keys[:burst.start_index] == burst.key_index)
            if earlier.size:
                last = int(earlier[-1])
                self.assertGreaterEqual(burst.start_index / rps - last / rps, cfg.burst_min_idle_sec)

    def test_bursts_off_means_no_burst_requests(self):
        plan = build_plan(N_POSTS, make_cfg(enable_burst=False, burst_rate=1.0, burst_share=1.0))

        self.assertEqual(len(plan.bursts), 0)
        self.assertEqual(plan.burst_requests, 0)

    def test_invalid_config_is_rejected(self):
        bad = [
            make_cfg(burst_share=1.5),
            make_cfg(burst_rate=-1.0),
            make_cfg(burst_duration_sec=0.0),
            make_cfg(requests_per_sec=0.0),
        ]
        for cfg in bad:
            with self.assertRaises(ValueError):
                build_plan(N_POSTS, cfg)
        with self.assertRaises(ValueError):
            build_plan(0, make_cfg())


if __name__ == "__main__":
    unittest.main()
