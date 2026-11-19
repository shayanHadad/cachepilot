"""Tests for compare_real.py (no model, no service). Run from evaluation/."""

import contextlib
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

import compare_real as cr
import offline_compare as oc

N_POSTS = 300


def make_posts_file(directory: Path) -> Path:
    path = directory / "media_sizes.json"
    path.write_text(json.dumps({
        "post_ids_sha256": "test",
        "ids": [f"{i:024x}" for i in range(N_POSTS)],
        "media_size_kb": [0.0 if i % 3 else 90.0 for i in range(N_POSTS)],
    }), encoding="utf-8")
    return path


def make_plan(seed: int = 1001, requests: int = 3000, burst: bool = True):
    args = Namespace(requests=requests, rps=100.0, zipf=0.9, burst_share=0.10)
    return oc.build_plan(N_POSTS, oc.make_config(seed, burst, args))


def write_log(path: Path, plan, hits, sources=None) -> None:
    ids = [f"{i:024x}" for i in range(N_POSTS)]
    with open(path, "w", encoding="utf-8") as f:
        for i, (key, hit) in enumerate(zip(plan.keys.tolist(), hits)):
            f.write(json.dumps({
                "timestamp": 1_000_000 + i * 10,
                "key": ids[key],
                "cache_status": "hit" if hit else "miss",
                "source": "cache-hit" if hit else (sources[i] if sources else "lru"),
            }) + "\n")


def run_main(*argv: str) -> tuple[int, str]:
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = cr.main(list(argv))
    return code, out.getvalue()


class HitSequenceTest(unittest.TestCase):
    def test_lru_matches_simulator_rate(self):
        rng = np.random.default_rng(1)
        keys = rng.integers(0, 40, size=2000).tolist()
        admit = (rng.random(2000) < 0.6).tolist()
        hits = cr.hit_sequence_lru(keys, admit, 10)
        self.assertAlmostEqual(sum(hits) / len(hits), oc.simulate_lru(keys, admit, 10).hit_rate)

    def test_lfu_matches_simulator_rate(self):
        rng = np.random.default_rng(2)
        keys = rng.integers(0, 60, size=3000).tolist()
        hits = cr.hit_sequence_lfu(keys, 15)
        self.assertAlmostEqual(sum(hits) / len(hits), oc.simulate_lfu(keys, 15).hit_rate)

    def test_lfu_hand_checked(self):
        self.assertEqual(cr.hit_sequence_lfu(list("aabca"), 2), [False, True, False, False, True])


class CompareHitsTest(unittest.TestCase):
    def test_counts_and_directions(self):
        real = [False, True, True, False, True]
        sim = [False, True, False, True, True]
        c = cr.compare_hits(real, sim, warmup=3)
        self.assertEqual(c["mismatches"], 2)
        self.assertEqual(c["first_mismatch"], 2)
        self.assertEqual(c["mismatches_after_warmup"], 1)
        self.assertEqual(c["real_hit_sim_miss"], 1)
        self.assertEqual(c["real_miss_sim_hit"], 1)
        self.assertAlmostEqual(c["real_hit_rate_all"], 3 / 5)
        self.assertAlmostEqual(c["real_hit_rate"], 1 / 2)

    def test_identical(self):
        c = cr.compare_hits([True, False], [True, False], 0)
        self.assertEqual(c["mismatches"], 0)
        self.assertIsNone(c["first_mismatch"])

    def test_length_mismatch(self):
        with self.assertRaises(ValueError):
            cr.compare_hits([True], [True, False], 0)


class LogReadingTest(unittest.TestCase):
    def test_skips_malformed_lines(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "service.jsonl"
            path.write_text('{"key": "a"}\n\n{"key": "b"\n{"key": "c"}\n', encoding="utf-8")
            records, skipped = cr.read_log(path)
        self.assertEqual([r["key"] for r in records], ["a", "c"])
        self.assertEqual(skipped, 1)

    def test_check_keys(self):
        rec = lambda *keys: [{"key": k} for k in keys]
        self.assertTrue(cr.check_keys(["a", "b"], rec("a", "b"))["complete"])
        short = cr.check_keys(["a", "b"], rec("a"))
        self.assertTrue(short["ok"])
        self.assertFalse(short["complete"])
        self.assertEqual(cr.check_keys(["a", "b"], rec("a", "x"))["first_mismatch"], 1)
        self.assertFalse(cr.check_keys(["a"], rec("a", "b"))["ok"])  # extra records, e.g. an appended log


class SourceBreakdownTest(unittest.TestCase):
    def test_fallbacks_in_first_requests_are_separated(self):
        records = []
        for i in range(300):
            if i < 4:
                records.append({"cache_status": "miss", "source": cr.FALLBACK_SOURCE})
            elif i % 2:
                records.append({"cache_status": "hit", "source": cr.HIT_SOURCE})
            else:
                records.append({"cache_status": "miss", "source": "lightgbm-x"})
        b = cr.source_breakdown(records, "lightgbm-x")
        self.assertEqual(b["fallbacks"], 4)
        self.assertEqual(b["fallbacks_in_first_requests"], 4)
        self.assertEqual(b["fallback_share_after_first_requests"], 0.0)
        self.assertTrue(b["valid"])
        self.assertEqual(b["unexpected_sources"], [])

    def test_late_fallbacks_invalidate_and_unknown_source_is_flagged(self):
        records = [{"cache_status": "miss", "source": "lightgbm-x"} for _ in range(100)]
        records += [{"cache_status": "miss", "source": cr.FALLBACK_SOURCE} for _ in range(10)]
        records += [{"cache_status": "miss", "source": "lightgbm-old"}]
        b = cr.source_breakdown(records, "lightgbm-x")
        self.assertFalse(b["valid"])
        self.assertEqual(b["unexpected_sources"], ["lightgbm-old"])


class MainBaselineTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.media = make_posts_file(self.dir)
        self.plan = make_plan()
        self.keys = self.plan.keys.tolist()

    def tearDown(self):
        self.tmp.cleanup()

    def args(self, log: Path, policy: str) -> list[str]:
        return ["--log", str(log), "--policy", policy, "--seed", "1001", "--burst", "--requests", "3000",
                "--capacity", "30", "--warmup", "300", "--media", str(self.media)]

    def test_identical_lru_run_passes(self):
        log = self.dir / "service.jsonl"
        write_log(log, self.plan, cr.hit_sequence_lru(self.keys, [True] * 3000, 30))
        code, out = run_main(*self.args(log, "lru"))
        self.assertEqual(code, 0, out)
        self.assertIn("IDENTICAL", out)

    def test_identical_lfu_run_passes(self):
        log = self.dir / "service.jsonl"
        write_log(log, self.plan, cr.hit_sequence_lfu(self.keys, 30), sources=["lfu"] * 3000)
        code, out = run_main(*self.args(log, "lfu"))
        self.assertEqual(code, 0, out)

    def test_one_flipped_hit_is_reported(self):
        hits = cr.hit_sequence_lru(self.keys, [True] * 3000, 30)
        hits[1234] = not hits[1234]
        log = self.dir / "service.jsonl"
        write_log(log, self.plan, hits)
        code, out = run_main(*self.args(log, "lru"))
        self.assertEqual(code, 1)
        self.assertIn("first at index 1234", out)

    def test_wrong_key_stops_before_comparing(self):
        hits = cr.hit_sequence_lru(self.keys, [True] * 3000, 30)
        log = self.dir / "service.jsonl"
        write_log(log, self.plan, hits)
        lines = log.read_text(encoding="utf-8").splitlines()
        rec = json.loads(lines[10])
        rec["key"] = f"{(int(rec['key'], 16) + 1) % N_POSTS:024x}"
        lines[10] = json.dumps(rec)
        log.write_text("\n".join(lines) + "\n", encoding="utf-8")
        code, out = run_main(*self.args(log, "lru"))
        self.assertEqual(code, 2)
        self.assertIn("first mismatch at index 10", out)

    def test_run_config_hash_is_checked(self):
        log = self.dir / "service.jsonl"
        write_log(log, self.plan, cr.hit_sequence_lru(self.keys, [True] * 3000, 30))
        good = self.dir / "run_config.json"
        good.write_text(json.dumps({"plan_sha256": self.plan.sha256}), encoding="utf-8")
        self.assertEqual(run_main(*self.args(log, "lru"), "--run-config", str(good))[0], 0)
        bad = self.dir / "other.json"
        bad.write_text(json.dumps({"plan_sha256": "0" * 64}), encoding="utf-8")
        self.assertEqual(run_main(*self.args(log, "lru"), "--run-config", str(bad))[0], 2)

    def test_training_seed_is_rejected(self):
        log = self.dir / "service.jsonl"
        write_log(log, self.plan, cr.hit_sequence_lru(self.keys, [True] * 3000, 30))
        argv = self.args(log, "lru")
        argv[argv.index("1001")] = "142"
        with self.assertRaises(ValueError):
            run_main(*argv)


class MainModelRunTest(unittest.TestCase):
    """The ml branch, with a stand-in for inference.decide (no lightgbm)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.media = make_posts_file(self.dir)
        self.model = self.dir / "model.txt"
        self.model.write_text("", encoding="utf-8")
        (self.dir / "model_meta.json").write_text(json.dumps({"model_name": "lightgbm-test"}), encoding="utf-8")
        self.plan = make_plan()
        self.keys = self.plan.keys.tolist()

        fake = types.ModuleType("model.inference")
        fake.init = lambda mode, path: None
        fake.decide = lambda key, f1, *rest: SimpleNamespace(admit=f1 >= 2)
        self.saved = {name: sys.modules.get(name) for name in ("model.inference",)}
        sys.modules["model.inference"] = fake
        import model
        self.had_attr = hasattr(model, "inference")
        model.inference = fake

    def tearDown(self):
        import model
        for name, mod in self.saved.items():
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod
        if not self.had_attr:
            del model.inference
        self.tmp.cleanup()

    def argv(self, log: Path) -> list[str]:
        return ["--log", str(log), "--policy", "ml", "--seed", "1001", "--burst", "--requests", "3000",
                "--capacity", "30", "--warmup", "300", "--media", str(self.media), "--model", str(self.model)]

    def model_admissions(self):
        feats = oc.compute_features(self.plan.keys, 100.0)
        return (feats.frequency_1min >= 2).tolist()

    def test_log_made_by_the_same_decisions_is_identical(self):
        hits = cr.hit_sequence_lru(self.keys, self.model_admissions(), 30)
        log = self.dir / "service.jsonl"
        write_log(log, self.plan, hits, sources=["lightgbm-test"] * 3000)
        code, out = run_main(*self.argv(log))
        self.assertEqual(code, 0, out)
        self.assertEqual(out.count("IDENTICAL"), 2, out)
        self.assertIn("VALID", out)

    def test_real_fallbacks_are_taken_from_the_log(self):
        admit = self.model_admissions()
        sources = ["lightgbm-test"] * 3000
        for i in range(4):  # lazy gRPC connection: the first misses fall back and admit
            admit[i] = True
            sources[i] = cr.FALLBACK_SOURCE
        hits = cr.hit_sequence_lru(self.keys, admit, 30)
        log = self.dir / "service.jsonl"
        write_log(log, self.plan, hits, sources=sources)
        code, out = run_main(*self.argv(log))
        self.assertEqual(code, 0, out)
        logged_section = out.split("logged timestamps and the real fallbacks")[1]
        self.assertIn("IDENTICAL", logged_section)


if __name__ == "__main__":
    unittest.main()
