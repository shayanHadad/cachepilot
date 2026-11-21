"""Tests for offline_compare.py (no model, no MongoDB). Run from evaluation/."""

import contextlib
import importlib.util
import io
import json
import sys
import tempfile
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


class WindowCountTest(unittest.TestCase):
    @staticmethod
    def brute_force(keys, times_ms, window_sec):
        out = []
        for i, key in enumerate(keys):
            now = times_ms[i] / 1000.0
            out.append(sum(1 for j in range(i) if keys[j] == key and times_ms[j] / 1000.0 > now - window_sec))
        return out

    def test_matches_brute_force(self):
        rng = np.random.default_rng(11)
        keys = rng.integers(0, 12, size=600)
        times = [int(i * 1000.0 / 4.0) for i in range(600)]
        for window in (5, 10, 60):
            np.testing.assert_array_equal(oc.window_count(keys, 4.0, window), self.brute_force(keys.tolist(), times, window))

    def test_sixty_seconds_equals_frequency_1min(self):
        rng = np.random.default_rng(12)
        keys = rng.integers(0, 25, size=3000)
        np.testing.assert_array_equal(
            oc.window_count(keys, 5.0, oc.ONE_MIN_SEC), oc.compute_features(keys, 5.0).frequency_1min)

    def test_boundary_is_strict_and_timestamps_can_be_given(self):
        keys = np.array([7, 7, 7])
        # gaps of exactly 10 s: the earlier request is not strictly newer than now - 10
        self.assertEqual(oc.window_count(keys, 1.0, 10, timestamps_ms=[0, 10_000, 20_000]).tolist(), [0, 0, 0])
        self.assertEqual(oc.window_count(keys, 1.0, 10, timestamps_ms=[0, 9_999, 19_000]).tolist(), [0, 1, 1])


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
        f10 = oc.window_count(plan.keys, args.rps, oc.SHORT_WINDOW_SEC)
        fake = lambda key, f1, *rest: SimpleNamespace(admit=f1 >= 2)
        res = oc.evaluate_plan(plan.keys, feats, f10, posts, fake, k=2, k10=2, capacity=50, warmup=200)

        self.assertEqual(set(res), set(oc.POLICIES))
        self.assertEqual(res["model"]["hit_rate"], res["rule"]["hit_rate"])
        self.assertEqual(res["model"]["hit_rate_all"], res["rule"]["hit_rate_all"])
        self.assertGreater(res["lru"]["hit_rate"], 0.0)
        self.assertLess(res["rule"]["admit_rate"], 1.0)
        self.assertEqual(res["lru"]["admit_rate"], 1.0)

        direct = oc.simulate_lru(plan.keys.tolist(), (f10 >= 2).tolist(), 50, 200)
        self.assertEqual(res["rule10"]["hit_rate"], direct.hit_rate)
        self.assertEqual(res["rule10"]["admit_rate"], direct.admit_rate)


class MainRunTest(unittest.TestCase):
    """main() end to end, with a stand-in for inference.decide (no lightgbm)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        n_posts = 300
        (self.dir / "media_sizes.json").write_text(json.dumps({
            "post_ids_sha256": "test",
            "ids": [f"{i:024x}" for i in range(n_posts)],
            "media_size_kb": [0.0 if i % 3 else 90.0 for i in range(n_posts)],
        }), encoding="utf-8")
        (self.dir / "model.txt").write_text("", encoding="utf-8")
        (self.dir / "model_meta.json").write_text(json.dumps({
            "model_name": "lightgbm-test", "model_sha256": "0" * 64,
            "admit_probability_threshold": 0.25, "capacity": 30}), encoding="utf-8")

        fake = types.ModuleType("model.inference")
        fake.init = lambda mode, path: None
        fake.decide = lambda key, f1, *rest: SimpleNamespace(admit=f1 >= 2)
        sys.modules["model.inference"] = fake
        import model
        self.had_attr = hasattr(model, "inference")
        model.inference = fake

    def tearDown(self):
        import model
        sys.modules.pop("model.inference", None)
        if not self.had_attr:
            del model.inference
        self.tmp.cleanup()

    def test_runs_and_reports_both_rules(self):
        out_json = self.dir / "result.json"
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            oc.main(["--media", str(self.dir / "media_sizes.json"), "--model", str(self.dir / "model.txt"),
                     "--seeds", "1001", "1002", "--requests", "3000", "--capacity", "30", "--warmup", "300",
                     "--out", str(out_json)])
        text = out.getvalue()
        self.assertIn("frequency_10s k=", text)
        self.assertIn("rule10 minus rule", text)
        data = json.loads(out_json.read_text(encoding="utf-8"))
        self.assertIn("rule10_k", data)
        first = data["results"]["burst"]["seeds"][0]["policies"]
        self.assertEqual(set(first), set(oc.POLICIES))
        self.assertIn("rule10_minus_rule", data["results"]["burst"]["after_warmup"])


if __name__ == "__main__":
    unittest.main()
