# CachePilot — Experiment Log

A log of formal, reproducible experiments. Each entry records the date,
the exact parameters, the hash of the commit that contains the code that
produced the result, the raw results and a one-line conclusion.
Throwaway debugging runs are not logged here, and an experiment is only
logged once the code that produced it is committed.

Record each experiment using this template:

```
## Experiment [number] — [short description]
**Date:** ...
**Commit hash:** (output of git rev-parse HEAD)

**Parameters:**
- cache.policy: lru / lfu / ml
- cache.capacity: ...
- workload: zipf_param=..., enable_burst=..., burst_rate=...,
  burst_share=..., burst_duration_sec=..., burst_min_idle_sec=...,
  seed=... (plan_sha256 and post_ids_sha256 from run_config.json)
- total number of requests: ...

**Raw result:**
- Hits: ...
- Misses: ...
- Evictions: ...
- Hit rate: ...
- Average latency: ...
- p95 latency: ...
- (related raw file, if saved: docs/raw-results/exp-XX.json)

**One-line conclusion:** ...
```

Training experiments (model fitting and its offline evaluation) use
this template instead:

```
## Experiment [number] — [short description]
**Date:** ...
**Commit hash:** (output of git rev-parse HEAD; train from a clean tree)

**Parameters:**
- seeded posts and workload command (including seed)
- label window (`--window-sec`), split fractions
- train.py settings and environment (library versions)

**Raw result:**
- dataset sizes (raw, labeled, train/fit/validation/test), class rates
- scale_pos_weight, best iteration, validation AUC
- test ROC AUC and reject average precision (with the random baseline)
- test report at the default and the tuned threshold
- feature importance and single-feature AUC
- related raw files: docs/raw-results/exp-XX-*.txt / *.json

**One-line conclusion:** ...
```

---

## Experiment 1 — First successful admit-model training run

**Date:** 2026-10-01 (workload recorded 16:02–16:12, dataset built at
16:15, model trained at 16:16, local time)
**Commit hash:** cc0b1a13827b5697b8eb23111abfe720c2a37e0a — the training
code (`ml-service/model/train.py`, first committed in `f6d14cf`) was not
yet committed when the original run was executed. Re-running it at this
commit on the same dataset reproduced every figure below exactly.

**Parameters:**

- Seeded posts: `python scripts/seed_posts.py --count 5000 --wipe`
- Workload: `python zipfian_generator.py --total-requests 60000
--requests-per-sec 100 --zipf-param 0.9 --seed 42` (burst disabled)
- cache.policy: lru (dataset-building run; policy doesn't affect the
  logged request stream's content, only which keys get admitted to
  the live cache during the run)
- cache.capacity: 250
- Training: `python train.py` with default settings (scale_pos_weight
  auto-computed from the fit set, up to 1000 boosting rounds, early
  stopping with 50 rounds patience; the training output reports
  `Evaluated only: auc`, so validation AUC is the effective stopping
  criterion and binary_logloss is tracked but does not trigger stopping)
- Environment: Python 3.11 with the versions pinned in
  `ml-service/requirements.txt` (lightgbm 4.5.0, scikit-learn 1.5.2,
  pandas 2.2.3, pyarrow 18.1.0)
- Raw request logs and the parquet datasets are stored locally under
  `data/`.

**Raw result:**

- Dataset: 59,993 raw log records → 29,993 labeled rows after
  dropping rows within 300s of the log's end (right-censoring trim) →
  23,994 train rows / 5,999 test rows
- Train rows split into 20,394 fit rows and 3,600 validation rows (132
  reject rows in validation)
- Train label_admit rate: 96.30%, test label_admit rate: 96.13%
  (closely matched, confirming the right-censoring fix is working)
- scale_pos_weight: 0.038
- Training stopped at round 83, best iteration 33
- Validation AUC: 0.8943 (validation reject F1 at the selected
  threshold: 0.2691)
- Test ROC AUC: 0.8990
- Test reject average precision: 0.1827 (random baseline: 0.0387)
- Test set, default threshold (0.5):
  - reject: precision 0.17, recall 0.78, f1 0.28
  - admit: precision 0.99, recall 0.84, f1 0.91
  - confusion matrix (rows = actual, cols = predicted; reject first):
    [[182, 50], [909, 4858]]
- Test set, tuned threshold (0.5219, selected on validation):
  - reject: precision 0.17, recall 0.78, f1 0.28
  - admit: precision 0.99, recall 0.85, f1 0.91
  - confusion matrix: [[180, 52], [876, 4891]]
- Feature importance (gain): frequency_5min 7385.7, frequency_1min
  691.9, recency_sec 413.0, inter_arrival_avg 345.1, payload_size_kb
  223.1, query_type 0.0
- Single-feature AUC on validation: frequency_5min 0.9046,
  frequency_1min 0.8647, inter_arrival_avg 0.6900, recency_sec 0.5195,
  payload_size_kb 0.5100 (full model: 0.8943)
- Related raw files: `docs/raw-results/exp-01-train-output.txt`,
  `docs/raw-results/exp-01-model-meta.json` (written before the model
  name, hash, training time and code version were added to the
  metadata, so it lacks those fields)

**One-line conclusion:** the model learns a real, non-trivial signal
(test AUC 0.899, ~4.7x better than random on the minority class), but
`frequency_5min` alone (validation AUC 0.9046) scores higher than the
full model (0.8943), so on this stationary, burst-free workload the
model adds no measurable signal beyond a one-feature rule; the
evaluation should include a `frequency_5min` threshold baseline. At the
tuned threshold the model predicts 1,056 rejects of which only 180 are
correct (precision 0.17), and the threshold was tuned on only 132
validation reject rows, so it is noisy and barely differs from 0.5. This
experiment says nothing yet about burst behavior; `query_type` and
`payload_size_kb` are close to useless on their own and are candidates
for a later feature-selection experiment.

**Note:** the train/test split and the fit/validation split have no gap
between their parts, so the metrics above may be slightly optimistic;
see "Time-Based Splits Without a Gap Between Train and Test" in
`architecture.md`.

**Note:** the label, the split and the threshold rule of this experiment were
later replaced (see "The Training Label Is Reuse Within the Cache's Reach", "Training Labels Never Depend on a Later Period (Exact Embargo)" and
"The Admission Threshold Is Chosen by Simulated Hit Rate" in `architecture.md`), and the model of this experiment is no
longer used.

---

## Experiment 2 — Admission model trained on the capacity-aware reuse label

**Date:** 2026-11-17 (workload recorded 16:16–16:26, dataset built and
model trained at 16:26, local time); reproduced on 2026-11-18
**Commit hash:** 88b868ce0c786ea6dbc197c01681c271c66a9680 — the pipeline
and training code were not yet committed when the original run was
executed. Re-running `labeling.py`, `time_split.py` and `train.py` at this
commit, on a clean working tree and the same features file, reproduced the
model byte for byte (`model_sha256` below).

**Parameters:**

- Posts: 5,000 in MongoDB (`post_ids_sha256`
  `08fd32d78b4d6ad273206a3772952be28428b7a5b1e0e570a7370cc828fc8203`)
- Workload (dataset-building run, cache.policy: lru, cache.capacity: 250):
  `python zipfian_generator.py --enable-burst --seed 142
--total-requests 60000 --requests-per-sec 100 --zipf-param 0.9
--output-root ../data/raw_train --go-service-log-path
../data/raw_train/service.jsonl` with the default burst settings
  (burst_rate 1/60, burst_share 0.10, burst_duration_sec 30,
  burst_min_idle_sec 10); `plan_sha256`
  `716d7caa181c30aec13eeaa9abf91e0e521428d3dfd18c59da942a64a0a36d55`
- Dataset: `python build_dataset.py --logs
"../data/raw_train/run_*/service.jsonl"`, `python labeling.py
--capacity 250`, `python time_split.py` (train fraction 0.8)
- Training: `python train.py` with default settings (learning rate 0.05,
  15 leaves, `min_child_samples` 50, seed 42, up to 1000 boosting rounds,
  early stopping with 50 rounds patience on validation AUC; validation is
  the last 15% of the train period by time; thresholds from 0.05 to 0.95 in
  steps of 0.05)
- Environment: Python 3.11 with the versions pinned in
  `ml-service/requirements.txt`
- Raw request logs and the parquet datasets are stored locally under
  `data/`.

**Raw result:**

- Dataset: 59,989 log records (one malformed line skipped: the cut last
  line of the service-log snapshot) → 59,739 labeled rows after dropping
  250 right-censored rows → 47,791 train / 11,948 test rows, of which 250
  train rows are flagged `label_leaks`. `media_size_kb` was found for all
  4,779 unique keys.
- `label_reuse` rate: 45.77% overall, 45.48% train, 46.89% test
- Fit rows 40,372 and validation rows 6,919; 500 train rows left out
  because their label reaches into a later period
- `scale_pos_weight`: 1.173; 152 rounds run, best iteration 102
  (validation AUC 0.949467)
- Admission threshold chosen on validation: 0.25. Validation hit rate:
  model 56.79% (admits 0.5% of misses), LRU 43.51%, rule
  `frequency_1min` >= 6 56.97%
- Single-feature AUC on validation: full model 0.9495, `frequency_1min`
  0.9445, `frequency_5min` 0.9456, `recency_sec` 0.8423 (raw 0.1577),
  `inter_arrival_avg` 0.7705 (raw 0.2295), `payload_size_kb` 0.5419 (raw
  0.4581)
- Test set: ROC AUC 0.9485, average precision 0.9541 (random baseline
  0.4689); at threshold 0.25 precision 0.785 and recall 0.931
- Simulated hit rate on the test period (LRU, capacity 250): LRU admitting
  everything 46.72%; rule `frequency_1min` >= 6 58.65% (admits 0.5% of
  misses); model at threshold 0.25 58.34% (admits 0.6% of misses)
- Feature importance (gain): `frequency_1min` 334047.9,
  `inter_arrival_avg` 24697.7, `frequency_5min` 6029.8, `recency_sec`
  4248.5, `payload_size_kb` 425.7, `query_type` 0.0
- Model: `lightgbm-40b5939e`, `model_sha256`
  `40b5939eb6a6d0a8c5127b3b7dd794a6af8f3b5c0b2e81326b8732340258d0db`
- Related raw files: `docs/raw-results/exp-02-train-output.txt`,
  `docs/raw-results/exp-02-labeling-output.txt`,
  `docs/raw-results/exp-02-split-output.txt`,
  `docs/raw-results/exp-02-model-meta.json` (written by the reproduction
  run, so its `trained_at` is 2026-11-18)

**One-line conclusion:** the capacity-aware label gives a model with
validation AUC 0.949 whose chosen threshold admits 0.6% of misses and
reaches 58.34% simulated hit rate against 46.72% for LRU on the last 20% of
the run, but it is 0.3 points below a one-feature rule on `frequency_1min`
(58.65%) and nearly all its signal is in `frequency_1min` (AUC 0.9445 alone
against 0.9495 for the whole model). The hit rates come from the LRU
simulation in `train.py`, not from a real cache run, and one 120 s test
segment says little about the comparison with the rule; the multi-seed
comparison and a check against the real system are still to be done.

<!-- Add real experiments here going forward -->
