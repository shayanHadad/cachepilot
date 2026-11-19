"""Tests for offline_compare.py (no model, no MongoDB). Run from evaluation/."""

import importlib.util
import sys
import types
import unittest
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import numpy as np

import offline_compare as oc
from workload_plan import build_plan

ROOT = Path(__file__).resolve().parents[1]


def load_build_dataset():
    """Imports data-pipeline/build_dataset.py, with a stub pymongo if the
    interpreter has none (the module imports it at the top)."""
    stubbed = importlib.util.find_spec("pymongo") is None
    if stubbed:
        sys.modules["pymongo"] = types.SimpleNamespace(MongoClient=None)
    try:
        spec = importlib.util.spec_from_file_location("build_dataset", ROOT / "data-pipeline" / "build_dataset.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        if stubbed:
            del sys.modules["pymongo"]


class FeatureParityTest(unittest.TestCase):
    def test_matches_build_dataset(self):
        try:
            bd = load_build_dataset()
        except ImportError as e:
            self.skipTest(f"build_dataset could not be imported: {e}")

        rng = np.random.default_rng(7)
        rps = 5.0  # 3,000 requests span 600 s, so the 5-minute window prunes
        keys = rng.integers(0, 25, size=3000)
        records = [
            {"key": str(int(k)), "timestamp": int(i * 1000.0 / rps), "query_type": "text_post"}
            for i, k in enumerate(keys)
        ]
        expected = bd.build_features(records, {})
        got = oc.compute_features(keys, rps)

        np.testing.assert_array_equal(got.frequency_1min, expected["frequency_1min"].to_numpy())
        np.testing.assert_array_equal(got.frequency_5min, expected["frequency_5min"].to_numpy())
        np.testing.assert_array_equal(got.recency_sec, expected["recency_sec"].to_numpy())
        np.testing.assert_array_equal(got.inter_arrival_avg, expected["inter_arrival_avg"].to_numpy())
        self.assertGreater(got.frequency_5min.max(), 10)  # the window was actually exercised


class ExplicitTimestampsTest(unittest.TestCase):
    def test_simulated_timestamps_give_identical_features(self):
        rng = np.random.default_rng(3)
        keys = rng.integers(0, 20, size=1500)
        rps = 5.0
        explicit = [int(i * 1000.0 / rps) for i in range(len(keys))]
        a = oc.compute_features(keys, rps)
        b = oc.compute_features(keys, rps, timestamps_ms=explicit)
        for name in ("frequency_1min", "frequency_5min", "recency_sec", "inter_arrival_avg"):
            np.testing.assert_array_equal(getattr(a, name), getattr(b, name))

    def test_jittered_timestamps_change_recency(self):
        keys = np.array([1, 1])
        f = oc.compute_features(keys, 100.0, timestamps_ms=[0, 37])
        self.assertAlmostEqual(f.recency_sec[1], 0.037)


class LfuTest(unittest.TestCase):
    def test_evicts_lowest_frequency(self):
        # a is hit twice, so it survives b and c; LRU would have dropped it.
        self.assertEqual(oc.simulate_lfu(list("aabca"), 2).hit_rate, 2 / 5)

    def test_ties_evict_least_recently_used(self):
        # a, b, c all have frequency 1; c evicts a, so b still hits.
        self.assertEqual(oc.simulate_lfu(list("abcb"), 2).hit_rate, 1 / 4)

    def test_frequency_resets_after_eviction(self):
        # a reaches 3, is never evicted while b/c churn; then d..e push a out only if it is lowest.
        res = oc.simulate_lfu(list("aaabcbc"), 2)
        self.assertEqual(res.requests, 7)
        self.assertGreater(res.hit_rate, 0)

    def test_count_from_and_capacity_check(self):
        self.assertEqual(oc.simulate_lfu(list("aaaa"), 2, count_from=1).hit_rate, 1.0)
        self.assertEqual(oc.simulate_lfu(list("aaaa"), 2, count_from=1).requests, 3)
        with self.assertRaises(ValueError):
            oc.simulate_lfu(list("a"), 0)


class TuneRuleTest(unittest.TestCase):
    def test_ties_go_to_lowest_k(self):
        keys = list(range(1000))  # every key unique: no k can hit
        best, rates = oc.tune_rule_k(keys, np.zeros(1000, dtype=np.int64), 10)
        self.assertEqual(best, 1)
        self.assertTrue(all(r == 0.0 for r in rates.values()))

    def test_picks_the_k_that_helps(self):
        # Half the traffic is one-off keys (frequency_1min 0) that flood a
        # 5-slot cache. k=0 admits them and loses the hot keys; k>=1 does not.
        rng = np.random.default_rng(0)
        n = 4000
        keys = np.where(rng.random(n) < 0.5, rng.integers(0, 5, n), 1000 + np.arange(n))
        feats = oc.compute_features(keys, 10.0)
        best, rates = oc.tune_rule_k(keys.tolist(), feats.frequency_1min, 5, ks=(0, 1, 2))
        self.assertGreater(rates[1], rates[0])
        self.assertEqual(best, 1)


class SummaryTest(unittest.TestCase):
    def test_paired_difference_and_wins(self):
        def row(model, lru):
            pol = {p: {"hit_rate": 0.5} for p in oc.POLICIES}
            pol["model"] = {"hit_rate": model}
            pol["lru"] = {"hit_rate": lru}
            return {"policies": pol}

        s = oc.summarize([row(0.60, 0.50), row(0.58, 0.50), row(0.49, 0.50)])
        d = s["paired"]["lru"]
        self.assertAlmostEqual(d["mean"], (0.10 + 0.08 - 0.01) / 3)
        self.assertAlmostEqual(d["sd"], float(np.std([0.10, 0.08, -0.01], ddof=1)))
        self.assertEqual(d["wins"], 2)
        self.assertEqual(s["n"], 3)

    def test_single_seed_has_zero_sd(self):
        pol = {p: {"hit_rate": 0.4} for p in oc.POLICIES}
        self.assertEqual(oc.summarize([{"policies": pol}])["sd"]["lru"], 0.0)


class SeedGuardTest(unittest.TestCase):
    def test_training_seeds_rejected(self):
        with self.assertRaises(ValueError):
            oc.validate_seeds([1001, 142])
        oc.validate_seeds(list(oc.EVAL_SEEDS))


class EndToEndTest(unittest.TestCase):
    def test_fake_model_equal_to_rule_gives_equal_hit_rates(self):
        n_posts = 300
        posts = oc.Posts(
            ids=[f"{i:024x}" for i in range(n_posts)],
            media_size_kb=[0.0 if i % 3 else 120.0 for i in range(n_posts)],
            query_types=["text_post" if i % 3 else "media_post" for i in range(n_posts)],
            ids_sha256="x",
        )
        args = Namespace(requests=4000, rps=100.0, zipf=0.9, burst_share=0.10)
        plan = build_plan(n_posts, oc.make_config(1001, True, args))
        self.assertEqual(len(plan.keys), 4000)
        self.assertEqual(plan.sha256, build_plan(n_posts, oc.make_config(1001, True, args)).sha256)

        feats = oc.compute_features(plan.keys, args.rps)
        fake = lambda key, f1, *rest: SimpleNamespace(admit=f1 >= 2)
        res = oc.evaluate_plan(plan.keys, feats, posts, fake, k=2, capacity=50, warmup=200)

        self.assertEqual(res["model"]["hit_rate"], res["rule"]["hit_rate"])
        self.assertEqual(res["model"]["hit_rate_all"], res["rule"]["hit_rate_all"])
        self.assertGreater(res["lru"]["hit_rate"], 0.0)
        self.assertLess(res["rule"]["admit_rate"], 1.0)
        self.assertEqual(res["lru"]["admit_rate"], 1.0)


if __name__ == "__main__":
    unittest.main()
