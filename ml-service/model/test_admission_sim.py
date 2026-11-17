"""
Tests for admission_sim. Run from ml-service/:

    python -m unittest model.test_admission_sim -v
"""

import unittest

import numpy as np

from model.admission_sim import best_cutoff, simulate_lru


class SimulateLruTest(unittest.TestCase):
    def test_plain_lru_by_hand(self):
        # capacity 2: a miss, b miss, a hit, c miss (evicts b), b miss (evicts a), a miss
        keys = list("abacba")
        result = simulate_lru(keys, [True] * len(keys), capacity=2)

        self.assertEqual(result.requests, 6)
        self.assertAlmostEqual(result.hit_rate, 1 / 6)
        self.assertEqual(result.admit_rate, 1.0)

    def test_nothing_admitted_means_no_hits(self):
        keys = list("aaaa")
        result = simulate_lru(keys, [False] * 4, capacity=2)

        self.assertEqual(result.hit_rate, 0.0)
        self.assertEqual(result.admit_rate, 0.0)

    def test_admission_can_beat_admit_all(self):
        # "a" keeps coming back; the one-off keys would evict it from a
        # cache of one slot unless they are rejected.
        keys = ["a", "x1", "a", "x2", "a", "x3", "a"]
        admit = [k == "a" for k in keys]

        self.assertGreater(
            simulate_lru(keys, admit, 1).hit_rate,
            simulate_lru(keys, [True] * len(keys), 1).hit_rate,
        )

    def test_only_requests_from_count_from_are_counted(self):
        keys = list("abab")
        result = simulate_lru(keys, [True] * 4, capacity=2, count_from=2)

        self.assertEqual(result.requests, 2)
        self.assertEqual(result.hit_rate, 1.0)  # the cache was warmed by the first two

    def test_invalid_input_is_rejected(self):
        with self.assertRaises(ValueError):
            simulate_lru(["a"], [True], capacity=0)
        with self.assertRaises(ValueError):
            simulate_lru(["a", "b"], [True], capacity=1)


class BestCutoffTest(unittest.TestCase):
    def test_picks_the_cutoff_that_keeps_the_hot_key(self):
        keys = ["a", "x1", "a", "x2", "a", "x3", "a"]
        scores = np.array([0.9 if k == "a" else 0.2 for k in keys])

        cutoff, result = best_cutoff(keys, scores, [0.1, 0.5, 0.95], capacity=1)

        self.assertEqual(cutoff, 0.5)  # 0.1 admits everything, 0.95 nothing
        self.assertAlmostEqual(result.hit_rate, 3 / 7)

    def test_ties_go_to_the_lowest_cutoff(self):
        keys = list("abab")
        scores = np.ones(4)

        cutoff, _ = best_cutoff(keys, scores, [0.2, 0.4, 0.6], capacity=2)

        self.assertEqual(cutoff, 0.2)

    def test_needs_at_least_one_cutoff(self):
        with self.assertRaises(ValueError):
            best_cutoff(["a"], np.ones(1), [], capacity=1)


if __name__ == "__main__":
    unittest.main()
