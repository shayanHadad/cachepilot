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

**Note:** the multi-seed comparison on independent seeds is Experiment 3.
The check against the real system is still open.

---

## Experiment 3 — Offline multi-seed policy comparison

**Date:** 2026-11-19
**Commit hash:** edcf65125a2f58653ba42c0ee0b0d6431cd53bd4 — the
`code_version` printed by the tool, run on a clean working tree (no
`+dirty` suffix). An earlier run on the same code before it was
committed (`code_version` `df5ae20…+dirty`) gave identical figures, so
the tool is deterministic.

**Parameters:**

- Command: `python -u evaluation/offline_compare.py --out
/tmp/exp-03.json`, with the ML service virtual environment. Post ids
  and media sizes came from `/usr/bin/python3
  evaluation/fetch_media_sizes.py` (natural collection order).
- Posts: 5,000 in MongoDB (`post_ids_sha256`
  `08fd32d78b4d6ad273206a3772952be28428b7a5b1e0e570a7370cc828fc8203`),
  1,787 of them with media
- Model: `lightgbm-40b5939e`, admission threshold 0.25, trained for
  cache.capacity 250 (Experiment 2). Policies are simulated (LRU of
  capacity 250 for all of them); no cache.policy run.
- Workload per run (simulated, `workload_plan.build_plan`): zipf_param
  0.9, 60,000 requests at 100 per second, evaluation seeds 1001 to 1008,
  each seed once stationary and once with bursts (burst_rate 1/60,
  burst_share 0.10, burst_duration_sec 30, burst_min_idle_sec 10); the
  per-seed `plan_sha256` values are in the raw JSON
- Hit rate counted after a warm-up of 2,500 requests (primary), and over
  all requests
- Rule baseline: `frequency_1min >= k`, `k` tuned on the training plan
  (seed 142, validation tail of the first 80%), never on an evaluated
  sequence; k = 6 (validation hit rates by k from 1 to 12: 48.68, 52.69,
  55.71, 56.38, 56.86, 57.21, 54.44, 52.36, 51.35, 49.29, 47.31, 45.93)
- LFU follows `lfu.go`: eviction before insertion, ties to the least
  recently used key (matched by reading the code)
- Environment: Python with the versions pinned in
  `ml-service/requirements.txt`

**Raw result:**

Hit rate after warm-up, mean over 8 seeds (sd in brackets), in %:

| Condition  | LRU          | LFU          | Rule (k=6)   | Model        |
| ---------- | ------------ | ------------ | ------------ | ------------ |
| Stationary | 43.03 (0.31) | 52.26 (0.37) | 53.63 (0.26) | 54.96 (0.25) |
| Bursts     | 45.76 (1.00) | 53.76 (0.83) | 55.45 (0.78) | 56.90 (0.77) |

Paired difference, model minus baseline, in points, mean (sd), seeds won
out of 8:

| Condition  | vs LRU             | vs LFU            | vs rule           |
| ---------- | ------------------ | ----------------- | ----------------- |
| Stationary | +11.93 (0.19), 8/8 | +2.69 (0.38), 8/8 | +1.33 (0.12), 8/8 |
| Bursts     | +11.14 (0.30), 8/8 | +3.14 (0.41), 8/8 | +1.45 (0.18), 8/8 |

Hit rate over all requests (warm-up included), mean over 8 seeds, in %:

| Condition  | LRU   | LFU   | Rule  | Model | Model − LRU | Model − LFU | Model − rule |
| ---------- | ----- | ----- | ----- | ----- | ----------- | ----------- | ------------ |
| Stationary | 42.96 | 51.95 | 52.46 | 54.23 | +11.26      | +2.28       | +1.76        |
| Bursts     | 45.60 | 53.41 | 54.24 | 56.11 | +10.51      | +2.71       | +1.88        |

- Misses the model admits: 1.16% to 1.43% per run after warm-up (stationary
  1.20, 1.21, 1.41, 1.35, 1.31, 1.28, 1.39, 1.22; bursts 1.18, 1.16, 1.43,
  1.41, 1.31, 1.22, 1.27, 1.23). Bursts per run: 12, 11, 9, 13, 6, 15, 6, 6
  for seeds 1001 to 1008.
- Reproduction check on the last 12,000 requests of the training plan
  (seed 142, which the model never saw): LRU 46.22, LFU 53.62, rule 58.24,
  model 57.99, with the model admitting 0.58% of misses. Experiment 2
  reported 46.72 (LRU), 58.65 (rule) and 58.34 (model) on its own test
  rows. The order of the policies matches and the offline figures are 0.4
  to 0.5 points lower; the cause of that offset is not established.
- Run time: about 140 s for the 17 plans.
- Related raw files: `docs/raw-results/exp-03-offline-compare.json`
  (written by the tool itself: per-seed results, plan fingerprints and
  `code_version`) and `docs/raw-results/exp-03-offline-compare.txt`. The
  text file is the program output as printed in the terminal; the `/tmp`
  copy of it was lost, so it was transcribed from the terminal.

**One-line conclusion:** on independent seeds the model beats LRU by about
11 to 12 points, LFU by about 2.7 to 3.1 and the tuned one-feature rule by
about 1.3 to 1.5 points, winning in all 16 paired runs; its advantage over
the rule is about the same with and without bursts (the difference between
the two conditions is 0.12 points and was not tested), so no burst-specific
advantage is measured. The figures come from a simulation (no timing, TTL,
timeouts or fallbacks) of one configuration and one capacity, the simulator
has not been compared with a real LRU run, and the LFU simulator follows
`lfu.go` by reading it, not by a live comparison. The model admits 1.2% to
1.4% of misses on these seeds after warm-up against 0.58% on the last 20% of
the training plan; the reason has not been investigated.

**Note:** the open points about the simulator (never compared with a real
run, LFU followed from reading `lfu.go`) were examined for seed 1001 in
Experiment 4.

**Note:** a later scratch study (not logged: its code is not in the
repository) found a stronger simple baseline than the rule used above,
"admit when `frequency_10s` >= 2". The model's lead over the best simple
rule is therefore smaller than the 1.3 to 1.5 points reported here; see
"Short-Window Frequency Features Did Not Help the Admission Model
(Scratch Study)" in `architecture.md`, and Experiment 5, which repeats
this comparison on the formal seeds with that rule included.

---

## Experiment 4 — Offline simulator against real service runs

**Date:** 2026-11-19
**Commit hash:** d83c5e4242f12df0f027575d4c0ebb2dfa468aae — the
`code_version` printed by `compare_real.py`, on a clean working tree (no
`+dirty` suffix). The Go service, the ML service and the tools ran from
this commit.

**Parameters:**

- Posts: 5,000 in MongoDB (`post_ids_sha256`
  `08fd32d78b4d6ad273206a3772952be28428b7a5b1e0e570a7370cc828fc8203`)
- Four real runs, each on its own empty log file (`config.yaml`
  unchanged; settings by environment variable:
  `CACHEPILOT_CACHE_POLICY`, `CACHEPILOT_CACHE_CAPACITY=250`,
  `CACHEPILOT_LOGGING_PATH`): `lru` with bursts, `lfu` with bursts, `ml`
  without bursts, `ml` with bursts. `lru` and `lfu` without bursts were
  not run: that was decided before the LFU and `ml` results were known,
  because the two algorithms do not depend on bursts.
- Workload (all runs): `python zipfian_generator.py [--enable-burst] --seed
1001 --total-requests 20000 --requests-per-sec 100 --zipf-param 0.9
--output-root ../data/eval_runs/<run> --go-service-log-path
../data/eval_runs/<run>/service.jsonl`, default burst settings.
  `plan_sha256` without bursts
  `c211f3b624d0cdc74fc9c849251b3f464b4fbab005e43ee920254279d09f4a7a`;
  with bursts
  `8c2325455459654d3a16d5077c26234be15ae5535f164fbd5ca4af89e31a0a14`
  (3 bursts, 895 burst requests, 4.47% of the traffic). Both matched the
  hashes in the runs' `run_config.json`.
- `ml` runs: model `lightgbm-40b5939e`, admission threshold 0.25, ML
  service on the same machine, started once before the first `ml` run and
  left running for the second.
- Comparison: `python evaluation/compare_real.py --log
data/eval_runs/<run>/service.jsonl --policy <lru|lfu|ml> --seed 1001
[--burst] --run-config data/eval_runs/<run>/run_*/run_config.json`.
  The service's live log was used, after stopping the Go service.
- Raw request logs and the comparison JSON files are stored locally under
  `data/eval_runs/` (not in the repository).

**Raw result:**

- All four runs: 20,000 log records, 0 malformed lines, logged keys equal
  to the planned keys, `plan_sha256` match. The workload summaries of
  `lru` (bursts), `ml` (no bursts) and `ml` (bursts) reported 0 failed
  requests; the summary of the `lfu` run was not recorded (20,000 aligned
  log records leave no room for a failed request).
- `lru`, bursts: hit rate after warm-up real 45.27%, simulated 45.27%; over
  all requests 44.87% and 44.87%; hit/miss sequences identical over all
  20,000 requests. Client latency avg/p50/p95/p99 4.89 / 5.18 / 6.36 /
  6.88 ms.
- `lfu`, bursts: 53.99% and 53.99% after warm-up; 52.94% and 52.94% over
  all requests; identical.
- `ml`, no bursts: sources 10,258 `cache-hit`, 9,726 `lightgbm-40b5939e`,
  16 `fallback-lru` (16 of 9,742 misses, all within the first 21 requests;
  share after the first 100 requests 0.00%, valid). Simulation as
  `offline_compare.py` does it: real 53.09% against simulated 53.05% after
  warm-up, 51.29% against 51.17% over all requests; 29 of 20,000 requests
  differ (0.14%), the first at index 7 and 12 after warm-up (27 real hit /
  simulated miss, 2 the other way). Simulation with the logged timestamps
  and the real fallbacks: 53.09% and 53.09%, identical sequences. Client
  latency avg/p50/p95/p99 5.94 / 4.95 / 8.77 / 9.25 ms.
- `ml`, bursts: sources 10,584 `cache-hit`, 9,414 `lightgbm-40b5939e`, 2
  `fallback-lru` (2 of 9,416 misses, requests 7,490 and 7,491; share after
  the first 100 requests 0.02%, valid). Simulation as `offline_compare.py`
  does it: real 55.05% against simulated 55.03% after warm-up, 52.92%
  against 52.90% over all requests; 10 of 20,000 requests differ (0.05%),
  the first at index 11,411 and all after warm-up (7 real hit / simulated
  miss, 3 the other way). Simulation with the logged timestamps and the
  real fallbacks: 55.05% and 55.05%, identical sequences. Client latency
  avg/p50/p95/p99 5.90 / 4.90 / 8.76 / 9.20 ms.
- Follow-up scratch check on the same logs (code not in the repository, so
  not a formal result): for both `ml` runs, adding the real fallbacks to
  the simulated decisions gave 0 differing requests with simulated time
  and with logged time; without them, 29 and 10. None of the 10 differing
  keys of the bursty run was a key admitted by a fallback.
- Related raw files: `docs/raw-results/exp-04-lru_burst.txt`,
  `exp-04-lfu_burst.txt`, `exp-04-ml_stat.txt`, `exp-04-ml_burst.txt`
  (the program output of each comparison)

**One-line conclusion:** on seed 1001 with 20,000 requests and capacity
250, the simulator reproduces the real service's hit/miss sequence exactly
for LRU and LFU, and for the model once the real fallbacks are included
(29 and 10 requests differ without them, a hit-rate gap of 0.04 and 0.02
points after warm-up), so the offline comparisons in Experiment 3 are not
distorted by the simulator on these runs. The check covers one seed, one
run length, one capacity and one model. Identical sequences do not prove
that every model decision matched, latency, TTL and timeouts were not
examined, and why the cold-started `ml` run had 16 fallbacks while the
warm one had none in its first 100 requests was not investigated.

---

## Experiment 5 — Offline comparison with a 10-second frequency rule

**Date:** 2026-11-21
**Commit hash:** a1fa67b5272283cab7ba1ca5d73c28318efc862c — the `code_version` printed by the tool,
on a clean working tree (no `+dirty` suffix). An earlier run of the same
code before it was committed (`code_version` `77255a4…+dirty`) gave
identical figures apart from the elapsed-time column.

**Parameters:**

- Same as Experiment 3, with one added baseline: `python -u
evaluation/offline_compare.py --out data/eval/exp-05.json`, the ML
  service virtual environment, post ids and media sizes from
  `evaluation/fetch_media_sizes.py`. Posts: 5,000
  (`post_ids_sha256`
  `08fd32d78b4d6ad273206a3772952be28428b7a5b1e0e570a7370cc828fc8203`);
  model `lightgbm-40b5939e` (threshold 0.25, capacity 250); zipf_param
  0.9, 60,000 requests at 100 per second, seeds 1001 to 1008, each
  stationary and with bursts (`burst_share` 0.10); 2,500 requests of
  warm-up excluded from the primary figures.
- New baseline `rule10`: admit when `frequency_10s` >= k, where
  `frequency_10s` is the number of earlier requests of the key strictly
  newer than 10 s, with the same millisecond time convention as
  `frequency_1min`. `k` = 2, tuned on the training plan (seed 142,
  validation tail of the first 80%), never on an evaluated sequence.
  Validation hit rates for k from 1 to 12: 53.32, 56.51, 53.42, 47.15,
  43.94, 40.43, 38.67, 35.96, 33.72, 32.00, 31.24, 29.39. The 1-minute
  rule is unchanged (`k` = 6).
- The 10-second rule exists only offline: the Go tracker has no
  10-second window.
- Origin of the rule: a scratch study (seeds 1101 to 1108, code not in the
  repository) found that it beat the 1-minute rule. Seeds 1001 to 1008
  were already used in Experiment 3. The model is fixed and `k` is tuned
  only on the training plan, so this reuse does not tune anything on the
  evaluated sequences.
- Checks before the run (scratch code, not in the repository): the
  10-second counts were recomputed with a different algorithm and were
  equal on all 60,000 requests of seed 1001 (stationary and bursts), the
  60-second window equals `frequency_1min`, and the rule's hit rates on
  seed 1001 and the tuning by `k` were reproduced independently.
- Environment: Python with the versions pinned in
  `ml-service/requirements.txt`.

**Raw result:**

- Regression check: the figures for LRU, LFU, the 1-minute rule and the
  model are identical to Experiment 3 in every printed digit (per seed and
  summaries), as is the reproduction check on seed 142 except for the new
  rule.
- Hit rate after warm-up, mean over 8 seeds (sd in brackets), in %:

| Condition  | LRU          | LFU          | Rule (1 min) | Rule10 (10 s) | Model        |
| ---------- | ------------ | ------------ | ------------ | ------------- | ------------ |
| Stationary | 43.03 (0.31) | 52.26 (0.37) | 53.63 (0.26) | 54.24 (0.28)  | 54.96 (0.25) |
| Bursts     | 45.76 (1.00) | 53.76 (0.83) | 55.45 (0.78) | 56.29 (0.76)  | 56.90 (0.77) |

- Paired difference after warm-up, in points, mean (sd), seeds won out of
  8:

| Condition  | model − rule10    | rule10 − rule     | model − rule (Exp. 3) |
| ---------- | ----------------- | ----------------- | --------------------- |
| Stationary | +0.72 (0.12), 8/8 | +0.61 (0.16), 8/8 | +1.33 (0.12), 8/8     |
| Bursts     | +0.61 (0.09), 8/8 | +0.84 (0.17), 8/8 | +1.45 (0.18), 8/8     |

- Hit rate over all requests (warm-up included), mean over 8 seeds, in %:

| Condition  | LRU   | LFU   | Rule  | Rule10 | Model | Model − rule10 | Rule10 − rule |
| ---------- | ----- | ----- | ----- | ------ | ----- | -------------- | ------------- |
| Stationary | 42.96 | 51.95 | 52.46 | 53.45  | 54.23 | +0.77          | +0.99         |
| Bursts     | 45.60 | 53.41 | 54.24 | 55.45  | 56.11 | +0.66          | +1.21         |

- Reproduction check on the last 12,000 requests of the training plan
  (seed 142, which the model never saw): LRU 46.22, LFU 53.62, rule 58.24,
  rule10 57.18, model 57.99. Here rule10 is below both the 1-minute rule
  and the model, although it is ahead of the 1-minute rule in all 16
  evaluated runs; the cause was not examined.
- Run time: about 141 s for the 17 plans.
- Related raw files: `docs/raw-results/exp-05-offline-compare.txt` and
  `docs/raw-results/exp-05-offline-compare.json`

**One-line conclusion:** a 10-second frequency rule beats the 1-minute
rule by 0.61 points on stationary traffic and 0.84 under bursts, in all 16
runs, and the model's lead over this stronger rule is 0.72 and 0.61
points, also in all 16 runs, so Experiment 3's lead of 1.3 to 1.5 points
overstated the model's margin over the best simple one-feature rule by
about half, while its lead over LRU (about 11 to 12 points) and LFU (about
2.7 to 3.1) is unchanged; the lead over the 10-second rule is not larger
under bursts (the 0.11-point difference between the conditions was not
tested), so no burst-specific advantage of the model is measured. The
figures come from a simulation (no timing, TTL, timeouts or fallbacks) of
one configuration and one capacity; the 10-second rule is offline only,
its `k` is sensitive (validation hit rate 56.51% at `k` = 2 against 53.42%
at 3 and 53.32% at 1), and it did worse than the model and the 1-minute
rule on the single 120 s segment of the seed-142 reproduction check, which
was not examined further.

<!-- Add real experiments here going forward -->
