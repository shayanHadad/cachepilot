"""
Trains a LightGBM classifier to predict label_admit.

label_admit skews heavily toward 1 (admit), so the minority class is
"reject". scale_pos_weight counters the imbalance during training;
early stopping uses validation AUC only, because unweighted logloss
disagrees with the weighted objective and stops training at round 1.
The decision threshold is tuned on the validation set (max F1 for the
reject class) rather than fixed at 0.5. See docs/architecture.md.

The validation set is the *tail* of the train set by time, not a random
sample, to avoid look-ahead leakage. The test set is used only once, for
final evaluation.

Usage:
    python train.py
"""

import argparse
import json
import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    confusion_matrix,
    precision_recall_curve,
    roc_auc_score,
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from features.feature_engineering import CATEGORICAL_COLUMNS, prepare_features


def time_based_validation_split(train_df: pd.DataFrame, val_fraction: float):
    train_df = train_df.sort_values("timestamp").reset_index(drop=True)
    split_idx = int(len(train_df) * (1 - val_fraction))
    return train_df.iloc[:split_idx], train_df.iloc[split_idx:]


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
    X_train, y_train = prepare_features(train_df), train_df["label_admit"]
    X_val, y_val = prepare_features(val_df), val_df["label_admit"]

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


def select_reject_threshold(model: lgb.Booster, val_df: pd.DataFrame):
    """Returns the reject-score threshold that maximizes reject-class F1 on validation."""
    X_val = prepare_features(val_df)
    y_reject = 1 - val_df["label_admit"].to_numpy()
    if y_reject.sum() == 0:
        raise ValueError("Validation set has no reject examples; can't tune a threshold.")

    reject_score = 1 - model.predict(X_val, num_iteration=model.best_iteration)
    precision, recall, thresholds = precision_recall_curve(y_reject, reject_score)
    precision, recall = precision[:-1], recall[:-1]
    denom = precision + recall
    f1 = np.divide(2 * precision * recall, denom, out=np.zeros_like(denom), where=denom > 0)
    best = int(np.argmax(f1))
    return float(thresholds[best]), float(f1[best])


def print_single_feature_auc(model: lgb.Booster, val_df: pd.DataFrame) -> None:
    """Leakage sanity check: validation AUC of each raw numeric feature on its own."""
    X_val, y_val = prepare_features(val_df), val_df["label_admit"]
    model_auc = roc_auc_score(y_val, model.predict(X_val, num_iteration=model.best_iteration))

    print("\n--- Single-feature AUC on validation (raw value as score) ---")
    print(f"  full model           {model_auc:.4f}")
    for col in X_val.columns:
        if col in CATEGORICAL_COLUMNS:
            continue
        mask = X_val[col].notna()
        auc = roc_auc_score(y_val[mask], X_val.loc[mask, col])
        print(f"  {col:20s} {max(auc, 1 - auc):.4f}  (raw {auc:.4f})")


def print_report(y_test: pd.Series, reject_score: np.ndarray, threshold: float, title: str) -> None:
    is_reject = reject_score >= threshold
    y_pred = (~is_reject).astype(int)

    print(f"\n{title} (reject if reject_score >= {threshold:.4f})")
    print(classification_report(
        y_test, y_pred, labels=[0, 1], target_names=["reject", "admit"], zero_division=0
    ))
    print("Confusion matrix (rows=actual, cols=predicted):")
    print(confusion_matrix(y_test, y_pred, labels=[0, 1]))


def evaluate(model: lgb.Booster, test_df: pd.DataFrame, threshold: float) -> None:
    X_test, y_test = prepare_features(test_df), test_df["label_admit"]
    p_admit = model.predict(X_test, num_iteration=model.best_iteration)
    reject_score = 1 - p_admit
    y_reject = 1 - y_test

    print("\n--- Test set evaluation ---")
    print(f"ROC AUC:                  {roc_auc_score(y_test, p_admit):.4f}")
    print(f"Reject average precision: {average_precision_score(y_reject, reject_score):.4f} "
          f"(random baseline = reject rate = {y_reject.mean():.4f})")

    print_report(y_test, reject_score, 0.5, "Default threshold")
    print_report(y_test, reject_score, threshold, "Tuned threshold (selected on validation)")


def print_feature_importance(model: lgb.Booster) -> None:
    importance = model.feature_importance(importance_type="gain")
    ranked = sorted(zip(model.feature_name(), importance), key=lambda x: -x[1])
    print("\n--- Feature importance (gain) ---")
    for name, score in ranked:
        print(f"  {name:20s} {score:.1f}")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--train", default="../../data/datasets/train.parquet")
    parser.add_argument("--test", default="../../data/datasets/test.parquet")
    parser.add_argument("--val-fraction", type=float, default=0.15,
                         help="Fraction of the train set's tail reserved for early-stopping validation")
    parser.add_argument("--learning-rate", type=float, default=0.05)
    parser.add_argument("--num-leaves", type=int, default=15)
    parser.add_argument("--min-child-samples", type=int, default=50)
    parser.add_argument("--out", default="artifacts/model.txt")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    train_df = pd.read_parquet(args.train)
    test_df = pd.read_parquet(args.test)
    print(f"Loaded {len(train_df)} train rows, {len(test_df)} test rows")

    fit_df, val_df = time_based_validation_split(train_df, args.val_fraction)
    print(f"Split train into {len(fit_df)} fit rows, {len(val_df)} validation rows "
          f"({int((val_df['label_admit'] == 0).sum())} reject in validation)")

    scale_pos_weight = compute_scale_pos_weight(fit_df["label_admit"])
    print(f"scale_pos_weight: {scale_pos_weight:.3f} (from fit set's class balance)")

    model = train_model(
        fit_df, val_df, scale_pos_weight,
        args.learning_rate, args.num_leaves, args.min_child_samples,
    )
    print(f"\nBest iteration: {model.best_iteration}")

    threshold, val_f1 = select_reject_threshold(model, val_df)
    print(f"Selected reject-score threshold: {threshold:.4f} (validation reject F1 = {val_f1:.4f})")

    print_single_feature_auc(model, val_df)
    evaluate(model, test_df, threshold)
    print_feature_importance(model)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    model.save_model(str(out_path))

    # The threshold is part of the decision rule, so it is saved next to the model.
    meta_path = out_path.with_name(out_path.stem + "_meta.json")
    meta_path.write_text(json.dumps({
        "best_iteration": model.best_iteration,
        "reject_score_threshold": threshold,
        "scale_pos_weight": scale_pos_weight,
        "feature_names": model.feature_name(),
    }, indent=2))
    print(f"\nSaved model to {out_path} and metadata to {meta_path}")


if __name__ == "__main__":
    main()
