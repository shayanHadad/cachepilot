# CachePilot — Experiment Log

A running log of benchmark and evaluation runs. Log even early
trial-and-error/debugging runs — they can be filtered out later, but
you can't reconstruct them after the fact.

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

**Date:** (fill in — date this run was actually executed)
**Commit hash:** TBD — fill in with `git rev-parse HEAD` once
`data-pipeline/` and `ml-service/model/train.py` are committed and
pushed; this result isn't reproducible from the repository until
that code exists there.

**Parameters:**

- Seeded posts: `python scripts/seed_posts.py --count 5000 --wipe`
- Workload: `python zipfian_generator.py --total-requests 60000
--requests-per-sec 100 --zipf-param 0.9 --seed 42` (burst disabled)
- cache.policy: lru (dataset-building run; policy doesn't affect the
  logged request stream's content, only which keys get admitted to
  the live cache during the run)
- cache.capacity: 250
- Training: `python train.py` with default settings (scale_pos_weight
  auto-computed, early stopping on auc + binary_logloss with 50
  rounds patience, max 500 boosting rounds)

**Raw result:**

- Dataset: 59,993 raw log records → 29,993 labeled rows after
  dropping rows within 300s of the log's end (right-censoring trim) →
  23,994 train rows / 5,999 test rows
- Train label_admit rate: 96.30%, test label_admit rate: 96.13%
  (closely matched, confirming the right-censoring fix is working)
- scale_pos_weight: 0.038
- Training stopped at round 83, best iteration 33
- Validation AUC: 0.8943
- Test ROC AUC: 0.8990
- Test reject average precision: 0.1827 (random baseline: 0.0387)
- Test set, default threshold (0.5):
  - reject: precision 0.17, recall 0.78, f1 0.28
  - admit: precision 0.99, recall 0.84, f1 0.91
- Test set, tuned threshold (0.5219, selected on validation):
  - reject: precision 0.17, recall 0.78, f1 0.28
  - admit: precision 0.99, recall 0.85, f1 0.91
- Feature importance (gain): frequency_5min 7385.7, frequency_1min
  691.9, recency_sec 413.0, inter_arrival_avg 345.1, payload_size_kb
  223.1, query_type 0.0
- Single-feature AUC: frequency_5min 0.9046, frequency_1min 0.8647,
  inter_arrival_avg 0.6900, recency_sec 0.5195, payload_size_kb 0.5100

**One-line conclusion:** the model learns a real, non-trivial signal
(test AUC 0.899, ~4.7x better than random on the minority class) once
early stopping was fixed to not halt after a single round;
`frequency_5min` alone carries almost all of the model's predictive
power, while `query_type` and `payload_size_kb` are close to useless
on their own — worth a feature-selection follow-up experiment later.

<!-- Add real experiments here going forward -->
