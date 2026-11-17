"""
Trains a LightGBM classifier that predicts label_reuse: will this key be
requested again before an LRU cache of `capacity` keys would have dropped
it? (See data-pipeline/labeling.py.)

The admission rule built on it is: admit a missed key when the predicted
probability is at least admit_probability_threshold. That threshold is
chosen by replaying the validation period through an LRU simulation
(model/admission_sim.py) and keeping the value with the highest hit rate,
not by a classification metric: a good F1 can admit almost nothing or
almost everything, and hit rate is what the cache is for. A one-feature
rule on frequency_1min is tuned the same way and reported next to the
model. See docs/architecture.md.

The validation set is the *tail* of the train set by time, not a random
sample. Training and validation rows whose labels depend on a later
period (label_leaks, and rows that end after the validation start) are
left out of fitting and scoring. The test set is used only once, for
final evaluation.

Usage:
    python train.py
"""

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from features.feature_engineering import CATEGORICAL_COLUMNS, prepare_features
from model.admission_sim import DEFAULT_THRESHOLDS, best_cutoff, simulate_lru

LABEL = "label_reuse"
REQUIRED_COLUMNS = ("key", "timestamp", LABEL, "label_end_ts", "label_capacity")
# Cutoffs tried for the one-feature baseline "admit when frequency_1min >= k".
RULE_CUTOFFS = tuple(range(1, 13))


def check_columns(df: pd.DataFrame, name: str, extra: tuple = ()) -> None:
    missing = [c for c in REQUIRED_COLUMNS + extra if c not in df.columns]
    if missing:
        raise ValueError(
            f"{name} is missing {missing}; rebuild the dataset with the current data-pipeline"
        )


def validation_split(train_df: pd.DataFrame, val_fraction: float):
    """Time-ordered split of the train set.

    Returns the sorted frame, the index where validation starts, and masks
    for the rows used to fit and to validate. A fit row is kept only if its
    label was certain before validation starts; rows flagged label_leaks
    are never used.
    """
    train_df = train_df.sort_values("timestamp", kind="stable").reset_index(drop=True)
    val_start = int(len(train_df) * (1 - val_fraction))
    position = np.arange(len(train_df))
    val_start_ts = train_df["timestamp"].iloc[val_start]

    clean = ~train_df["label_leaks"].to_numpy()
    fit_mask = clean & (position < val_start) & (train_df["label_end_ts"].to_numpy() < val_start_ts)
    val_mask = clean & (position >= val_start)
    return train_df, val_start, fit_mask, val_mask


def compute_scale_pos_weight(y: pd.Series) -> float:
    positives = y.sum()
    negatives = len(y) - positives
    if positives == 0 or negatives == 0:
        raise ValueError(
            f"Can't compute scale_pos_weight: {positives} positive, {negatives} negative "
            f"examples in training data — need both classes present."
        )
    return negatives / positives


def train_model(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    scale_pos_weight: float,
    learning_rate: float,
    num_leaves: int,
    min_child_samples: int,
) -> lgb.Booster:
    X_train, y_train = prepare_features(train_df), train_df[LABEL]
    X_val, y_val = prepare_features(val_df), val_df[LABEL]

    train_set = lgb.Dataset(X_train, label=y_train, categorical_feature=CATEGORICAL_COLUMNS)
    val_set = lgb.Dataset(X_val, label=y_val, categorical_feature=CATEGORICAL_COLUMNS, reference=train_set)

    # auc must stay first: early stopping only watches the first metric.
    params = {
        "objective": "binary",
        "metric": ["auc", "binary_logloss"],
        "scale_pos_weight": scale_pos_weight,
        "learning_rate": learning_rate,
        "num_leaves": num_leaves,
        "min_child_samples": min_child_samples,
        "seed": 42,
        "verbosity": -1,
    }

    return lgb.train(
        params,
        train_set,
        num_boost_round=1000,
        valid_sets=[val_set],
        callbacks=[
            lgb.early_stopping(stopping_rounds=50, first_metric_only=True),
            lgb.log_evaluation(period=1),
        ],
    )


def score(model: lgb.Booster, df: pd.DataFrame) -> np.ndarray:
    """Probability that each row's key is requested again within the cache's reach."""
    return model.predict(prepare_features(df), num_iteration=model.best_iteration)


def print_single_feature_auc(model: lgb.Booster, val_df: pd.DataFrame) -> None:
    """Leakage sanity check: validation AUC of each raw numeric feature on its own."""
    X_val, y_val = prepare_features(val_df), val_df[LABEL]
    model_auc = roc_auc_score(y_val, score(model, val_df))

    print("\n--- Single-feature AUC on validation (raw value as score) ---")
    print(f"  full model           {model_auc:.4f}")
    for col in X_val.columns:
        if col in CATEGORICAL_COLUMNS:
            continue
        mask = X_val[col].notna()
        auc = roc_auc_score(y_val[mask], X_val.loc[mask, col])
        print(f"  {col:20s} {max(auc, 1 - auc):.4f}  (raw {auc:.4f})")


def print_feature_importance(model: lgb.Booster) -> None:
    importance = model.feature_importance(importance_type="gain")
    ranked = sorted(zip(model.feature_name(), importance), key=lambda x: -x[1])
    print("\n--- Feature importance (gain) ---")
    for name, value in ranked:
        print(f"  {name:20s} {value:.1f}")


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def current_code_version() -> str | None:
    """Returns the current git commit, with a "+dirty" suffix when the
    working tree has uncommitted changes, or None if git isn't usable."""
    here = Path(__file__).resolve().parent
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=here, capture_output=True, text=True, check=True,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=here, capture_output=True, text=True, check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None
    return commit + ("+dirty" if status else "")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--train", default="../../data/datasets/train.parquet")
    parser.add_argument("--test", default="../../data/datasets/test.parquet")
    parser.add_argument("--val-fraction", type=float, default=0.15,
                        help="Fraction of the train set's tail reserved for validation")
    parser.add_argument("--learning-rate", type=float, default=0.05)
    parser.add_argument("--num-leaves", type=int, default=15)
    parser.add_argument("--min-child-samples", type=int, default=50)
    parser.add_argument("--out", default="artifacts/model.txt")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    train_df = pd.read_parquet(args.train)
    test_df = pd.read_parquet(args.test)
    check_columns(train_df, args.train, extra=("label_leaks",))
    check_columns(test_df, args.test)
    capacity = int(train_df["label_capacity"].iloc[0])
    print(f"Loaded {len(train_df)} train rows, {len(test_df)} test rows (label capacity {capacity})")

    train_df, val_start, fit_mask, val_mask = validation_split(train_df, args.val_fraction)
    fit_df, val_df = train_df[fit_mask], train_df[val_mask]
    skipped = len(train_df) - int(fit_mask.sum()) - int(val_mask.sum())
    print(f"Fit rows {len(fit_df)}, validation rows {len(val_df)}, "
          f"{skipped} rows left out because their label reaches into a later period")

    scale_pos_weight = compute_scale_pos_weight(fit_df[LABEL])
    print(f"scale_pos_weight: {scale_pos_weight:.3f} (from fit set's class balance)")

    model = train_model(
        fit_df, val_df, scale_pos_weight,
        args.learning_rate, args.num_leaves, args.min_child_samples,
    )
    print(f"\nBest iteration: {model.best_iteration}")

    # Replay the whole train period (it is contiguous) and count hits on the
    # validation tail only, so the cache is warm when counting starts.
    keys = train_df["key"].to_numpy()
    p_train = score(model, train_df)
    threshold, val_model = best_cutoff(keys, p_train, DEFAULT_THRESHOLDS, capacity, val_start)
    val_lru = simulate_lru(keys, np.ones(len(keys), dtype=bool), capacity, val_start)
    rule_k, val_rule = best_cutoff(
        keys, train_df["frequency_1min"].to_numpy(), RULE_CUTOFFS, capacity, val_start
    )
    print(f"Admission threshold chosen on validation: {threshold:.2f} "
          f"(hit rate {val_model.hit_rate:.2%}, admits {val_model.admit_rate:.1%} of misses)")
    print(f"  validation hit rate: LRU {val_lru.hit_rate:.2%} | "
          f"rule frequency_1min >= {rule_k:g} {val_rule.hit_rate:.2%} | model {val_model.hit_rate:.2%}")

    print_single_feature_auc(model, val_df)

    # Test: label metrics, then the cache simulation over train + test.
    p_test = score(model, test_df)
    y_test = test_df[LABEL].to_numpy()
    predicted = p_test >= threshold
    precision = float(y_test[predicted].mean()) if predicted.any() else float("nan")
    recall = float(predicted[y_test == 1].mean()) if (y_test == 1).any() else float("nan")
    print("\n--- Test set evaluation ---")
    print(f"ROC AUC: {roc_auc_score(y_test, p_test):.4f}")
    print(f"Average precision: {average_precision_score(y_test, p_test):.4f} "
          f"(random baseline = reuse rate = {y_test.mean():.4f})")
    print(f"At threshold {threshold:.2f}: precision {precision:.3f}, recall {recall:.3f}")

    all_keys = np.concatenate([keys, test_df["key"].to_numpy()])
    all_scores = np.concatenate([p_train, p_test])
    freq1 = np.concatenate([train_df["frequency_1min"].to_numpy(), test_df["frequency_1min"].to_numpy()])
    test_start = len(train_df)
    test_lru = simulate_lru(all_keys, np.ones(len(all_keys), dtype=bool), capacity, test_start)
    test_rule = simulate_lru(all_keys, freq1 >= rule_k, capacity, test_start)
    test_model = simulate_lru(all_keys, all_scores >= threshold, capacity, test_start)
    print(f"\n--- Simulated hit rate on the test period (LRU, capacity {capacity}) ---")
    print(f"  LRU, admit everything:           {test_lru.hit_rate:.2%}")
    print(f"  rule frequency_1min >= {rule_k:<2g}:      {test_rule.hit_rate:.2%}  (admits {test_rule.admit_rate:.1%} of misses)")
    print(f"  model at threshold {threshold:.2f}:       {test_model.hit_rate:.2%}  (admits {test_model.admit_rate:.1%} of misses)")

    print_feature_importance(model)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    model.save_model(str(out_path))

    # The threshold is part of the decision rule, so it is saved next to the model.
    # model_name is derived from the model file's hash, so it identifies the exact
    # model that produced a decision; the serving side reports it as the decision source.
    digest = file_sha256(out_path)
    meta_path = out_path.with_name(out_path.stem + "_meta.json")
    meta_path.write_text(json.dumps({
        "model_name": f"lightgbm-{digest[:8]}",
        "model_sha256": digest,
        "trained_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "code_version": current_code_version(),
        "best_iteration": model.best_iteration,
        "admit_probability_threshold": threshold,
        "label": LABEL,
        "capacity": capacity,
        "validation_hit_rate": val_model.hit_rate,
        "validation_lru_hit_rate": val_lru.hit_rate,
        "scale_pos_weight": scale_pos_weight,
        "feature_names": model.feature_name(),
    }, indent=2))
    print(f"\nSaved model to {out_path} and metadata to {meta_path}")


if __name__ == "__main__":
    main()