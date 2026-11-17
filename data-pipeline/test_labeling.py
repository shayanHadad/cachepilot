"""
Tests for labeling.py and time_split.py. Run from data-pipeline/:

    python -m unittest -v
"""

import random
import unittest

import numpy as np
import pandas as pd

from labeling import add_labels, reuse_labels
from time_split import time_split


def brute_force(keys: list, capacity: int):
    """The definition, written the slow way: (label, end_index, determined) per request."""
    out = []
    for i, key in enumerate(keys):
        later = keys[i + 1:]
        nxt = later.index(key) + i + 1 if key in later else None
        # The request that brings the distinct keys since i up to `capacity`.
        full = None
        seen = set()
        for j in range(i + 1, len(keys)):
            seen.add(keys[j])
            if len(seen) == capacity:
                full = j
                break
        if nxt is not None:
            others = len(set(keys[i + 1:nxt]))
            if others < capacity:
                out.append((1, nxt, True))
            else:
                out.append((0, full, True))
        elif full is not None:
            out.append((0, full, True))
        else:
            out.append((0, -1, False))
    return out


class ReuseLabelsTest(unittest.TestCase):
    def test_hand_made_cases(self):
        a = reuse_labels(list("aba"), capacity=2)
        self.assertEqual((a.label[0], a.end_index[0], a.determined[0]), (1, 2, True))

        b = reuse_labels(list("abca"), capacity=2)
        self.assertEqual((b.label[0], b.end_index[0], b.determined[0]), (0, 2, True))
        # The last request has no next one and nothing follows: not certain.
        self.assertFalse(b.determined[3])

    def test_matches_brute_force_on_random_sequences(self):
        rng = random.Random(1)
        for _ in range(40):
            keys = [rng.randrange(12) for _ in range(rng.randrange(1, 120))]
            capacity = rng.randrange(1, 6)
            result = reuse_labels(keys, capacity)
            for i, (label, end, determined) in enumerate(brute_force(keys, capacity)):
                self.assertEqual(bool(result.determined[i]), determined, (keys, capacity, i))
                if determined:
                    self.assertEqual(int(result.label[i]), label, (keys, capacity, i))
                    self.assertEqual(int(result.end_index[i]), end, (keys, capacity, i))

    def test_label_is_the_lru_hit_condition(self):
        # With every key admitted, label_reuse == "the next request is an LRU hit".
        rng = random.Random(2)
        keys = [rng.randrange(30) for _ in range(500)]
        capacity = 5
        cache, hits = [], {}
        for i, key in enumerate(keys):
            if key in cache:
                cache.remove(key)
                hits[i] = True
            else:
                hits[i] = False
                if len(cache) >= capacity:
                    cache.pop(0)
            cache.append(key)
        result = reuse_labels(keys, capacity)
        for i in range(len(keys)):
            if result.next_index[i] >= 0:
                self.assertEqual(bool(result.label[i]), hits[result.next_index[i]])

    def test_rejects_invalid_capacity(self):
        with self.assertRaises(ValueError):
            reuse_labels(["a"], 0)


class AddLabelsTest(unittest.TestCase):
    def make_df(self, keys):
        return pd.DataFrame({
            "key": keys,
            "timestamp": [1_000 + 10 * i for i in range(len(keys))],
        })

    def test_columns_and_censored_rows(self):
        df = add_labels(self.make_df(list("abcabc") + ["d"]), capacity=2)

        self.assertTrue({"label_reuse", "label_end_ts", "label_capacity", "label_ttl_sec"} <= set(df.columns))
        self.assertTrue((df["label_capacity"] == 2).all())
        # 'c' (index 5) and 'd' (index 6) are never requested again and fewer
        # than 2 distinct keys follow them: both are dropped.
        self.assertEqual(len(df), 5)
        self.assertTrue((df["label_end_ts"] >= df["timestamp"]).all())

    def test_time_to_next_request_in_seconds(self):
        df = add_labels(self.make_df(list("abab") + ["x", "y", "z"]), capacity=3)
        self.assertAlmostEqual(df.loc[0, "label_ttl_sec"], 0.02)
        self.assertTrue(np.isnan(df.loc[df["key"] == "y", "label_ttl_sec"]).all())

    def test_everything_censored_is_an_error(self):
        with self.assertRaises(RuntimeError):
            add_labels(self.make_df(list("abc")), capacity=10)


class TimeSplitTest(unittest.TestCase):
    def test_flags_training_labels_that_reach_into_the_test_period(self):
        df = pd.DataFrame({
            "timestamp": [100, 200, 300, 400, 500, 600],
            "label_reuse": [1, 0, 1, 0, 1, 0],
            "label_end_ts": [150, 450, 350, 650, 550, 650],
        })

        train, test = time_split(df, 0.5)

        self.assertEqual(train["timestamp"].tolist(), [100, 200, 300])
        self.assertEqual(test["timestamp"].tolist(), [400, 500, 600])
        # The test period starts at 400: only the row ended at 450 reaches it.
        self.assertEqual(train["label_leaks"].tolist(), [False, True, False])

    def test_rejects_an_empty_side(self):
        df = pd.DataFrame({"timestamp": [1, 2], "label_end_ts": [1, 2]})
        with self.assertRaises(ValueError):
            time_split(df, 1.0)


if __name__ == "__main__":
    unittest.main()