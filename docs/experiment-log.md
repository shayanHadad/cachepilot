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
  burst_intensity=..., burst_duration=..., seed=...
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
  auto-computed from the fit set, up to 500 boosting rounds, early
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
  `docs/raw-results/exp-01-model-meta.json`

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

<!-- Add real experiments here going forward -->
