# CachePilot — Alternatives Considered and Rejected

This document tracks design alternatives that were evaluated during
development but ultimately not chosen, along with the reasoning.
Keeping this record makes future revisits of these decisions faster —
if a rejected approach starts to look attractive again later, this is
where to check what was already ruled out and why.

Record each entry using this template:

```
## [Short decision title]
**Date:** ...

**Rejected alternative:** ...
**Why it was considered:** ...
**Why it was rejected:** ...
**Revisit if:** (conditions under which this alternative might be worth reconsidering)
```

---

## Online learning for the ML decision model

**Date:** 2026-08 (project start)

**Rejected alternative:** Online model training (the model would be
updated with every new request), instead of offline training with
inference-only at runtime.

**Why it was considered:** It seemed like it could react faster to
changes in the access pattern (e.g. sudden bursts in popularity).

**Why it was rejected:**

- Inference needs to be fast on the critical cache-miss path; online
  training would slow this path down.
- Reproducibility becomes difficult (the model would behave
  differently on every run, making results hard to compare across
  runs).
- Explainability (one of the reasons LightGBM/XGBoost was chosen)
  would be lost with a model that's constantly changing.

**Revisit if:** A hybrid approach — periodic retraining (not
per-request) on a short interval, outside the critical path — could
be worth exploring if the offline-trained model's staleness becomes a
measurable problem.

---

## TLS on the Go ↔ ML Service gRPC Connection

**Date:** 2026-08-21

**Rejected alternative:** Encrypting the gRPC connection between the Go
service and the ML service with TLS.

**Why it was considered:** Encrypting service-to-service traffic is the
usual default.

**Why it was rejected:**

- The connection is only ever made over the internal docker-compose
  network, never a public one, so TLS's threat model (network
  eavesdropping, server impersonation) does not apply.
- The added TLS cost (handshake and encryption) is not worth paying on
  a path with a strict 5–10 ms budget.

**Revisit if:** The Go and ML services are ever deployed on separate,
untrusted networks instead of a single docker-compose stack. At that
point the decision should be re-evaluated from scratch, not just
patched.

---

## Writing the Workload Generator in Go Instead of Python

**Date:** 2026-08-22

**Rejected alternative:** Writing the request generator (Zipfian
sampling, rate limiting, burst injection) in Go instead of Python.

**Why it was considered:** Go's timing precision for rate limiting is
generally better than Python's (no GIL/GC-related jitter), and Go's
goroutines scale to high concurrent request rates more naturally than
Python's threading/asyncio model — relevant if request rates need to
go much higher during later benchmarking.

**Why it was rejected:**

- The rest of the offline pipeline (`data-pipeline/`, `evaluation/`,
  `analysis/`) is entirely Python; keeping the generator in Python
  keeps every script that reads/writes the same JSONL files in one
  language, rather than splitting the toolchain for no immediate
  benefit.
- Python's `numpy`/`pandas` ecosystem makes implementing and later
  extending the statistical side (Zipfian sampling, and any future
  distributions) meaningfully less work than reimplementing the same
  logic in Go.
- At the request rates actually used during development (tens of
  requests/second), Python's timing precision — using
  `time.monotonic()`-based absolute scheduling rather than naive
  accumulated `sleep()` calls — is more than sufficient; Go's timing
  advantage only matters at rates far higher than what this project
  currently generates.

**Revisit if:** final benchmark runs need request rates high enough
(likely in the thousands per second) that the generator's own timing
overhead starts measurably affecting latency results. At that point,
a separate Go-based generator built specifically for the final
high-rate benchmark run — not a full rewrite of the development-time
generator — would be the narrower, lower-risk option.

## Silent Fallback to the Heuristic Inside the ML Service

**Date:** 2026-10-10

Falling back to the heuristic when the model cannot be loaded, or adding a
third `model-or-heuristic` mode, was considered and rejected. It makes
"ml" results a mix of deciders without any visible sign, and Go already
provides `fallback-lru`.

**Revisit if:** availability matters more than measurement validity (for
example a production deployment) and Go's fallback proves insufficient.

## Hardcoded Decision Source Name

**Date:** 2026-10-10

A constant such as `lightgbm-v1` was considered for `DecisionResponse.source`.
It cannot distinguish a retrained model from the previous one.

**Revisit if:** model versions are managed by an external registry that
assigns names.

## Loading ML Service Settings From a `.env` File

**Date:** 2026-10-10

Rejected for now. The ML service reads `config.yaml` with defaults in code,
the same pattern the Go service uses, and does not need a second mechanism.

**Revisit if:** deployment introduces secrets or per-environment settings
that should not live in `config.yaml`.

---

## A TTL Parameter on `Cache.Put`

**Date:** 2026-08-20

**Rejected alternative:** Passing a TTL into `Cache.Put` so each
implementation enforces expiry itself.

**Why it was considered:** The "ml" policy needs a per-entry lifetime,
and `Put` is the natural place to hand it over.

**Why it was rejected:** LRU and LFU are meant to be pure baselines with
no TTL behavior. Two of the three implementations would accept the
parameter and ignore it, which is interface pollution. TTL is tracked
in `Manager` instead, and the interface gained only `Delete(key)` later.

**Revisit if:** A second policy needs TTL semantics, or the baselines
are extended with TTL variants.

---

## Marshaling Decoded Mongo Documents Directly

**Date:** 2026-08-20

**Rejected alternative:** Decoding a post into a generic map and
marshaling that map straight to JSON.

**Why it was considered:** Less code, and no second struct to keep in
sync with the document schema.

**Why it was rejected:** The driver's `_id` and date types implement
`MarshalJSON` and emit Extended JSON (`{"$oid": ...}`, `{"$date": ...}`)
with no error or warning. The bug only appears downstream, when a
consumer expects a scalar and gets a nested object.

**Revisit if:** The driver's default JSON encoding becomes plain, or the
number of document fields makes keeping two structs in sync a real
maintenance burden.

---

## Flushing the Log Buffer From a Second Goroutine

**Date:** 2026-08-22

**Rejected alternative:** A separate goroutine that calls `Flush()` on
its own timer.

**Why it was considered:** The simplest way to add periodic flushing
without touching the writer loop.

**Why it was rejected:** `bufio.Writer` is not safe for concurrent use,
so the flush would race with in-progress writes. The writer goroutine
now uses `select` over the entries channel and a ticker instead.

**Revisit if:** The writer becomes mutex-guarded or is replaced by a
concurrency-safe sink.

---

## Separate "Record" and "Compute" Calls in the Access Tracker

**Date:** 2026-08-20

**Rejected alternative:** Two tracker methods, one to record an access
and one to compute statistics.

**Why it was considered:** It is the obvious API split.

**Why it was rejected:** Recording the current access before computing
its own statistics makes recency about zero on every call, silently.
`Observe` computes from the history before appending, so the correct
order is the only possible one.

**Revisit if:** A caller needs statistics without recording an access.
Add a separate read-only method then, rather than splitting `Observe`.

---

## Configurable Retention Windows in the Access Tracker

**Date:** 2026-08-20

**Rejected alternative:** Passing the retention window to the tracker's
constructor.

**Why it was considered:** Flexibility for experiments.

**Why it was rejected:** A retention shorter than five minutes would
silently undercount `frequency_5min`. The one- and five-minute windows
are constants instead.

**Revisit if:** Experiments need different windows. Validate any new
value against the five-minute minimum when it is introduced.

---

## Random Train/Test Split

**Date:** 2026-10-03

**Rejected alternative:** Splitting the labeled dataset into train and
test sets at random.

**Why it was considered:** It is the default and keeps class balance
equal in both parts.

**Why it was rejected:** A random split trains on rows that happened
after some test rows, which never happens at serving time, so the test
score would overstate performance on unseen future traffic.

**Revisit if:** Evaluation moves to cross-validation over independent
workload runs, with each run as its own fold.

---

## Fixed 0.5 Decision Threshold

**Date:** 2026-10-03

**Rejected alternative:** Rejecting when the reject score reaches 0.5
instead of tuning the threshold.

**Why it was considered:** No tuning step, no extra value to store.

**Why it was rejected:** The target is heavily imbalanced, so 0.5 is not
necessarily where precision and recall trade off best for the minority
class. The threshold is tuned on the validation set and saved with the
model.

**Revisit if:** Tuning keeps giving no gain. In Experiment 1 the tuned
threshold (0.5219) was close to 0.5 and gave near-identical test
metrics; with a larger validation set that still holds, drop the tuning
step to remove complexity.

---

## `numpy.random.zipf` for Key Popularity

**Date:** 2026-08-22

**Rejected alternative:** Sampling post ranks directly with numpy's Zipf
generator.

**Why it was considered:** One call and no weight array to build.

**Why it was rejected:** Its range is unbounded, so it can return a rank
higher than the number of posts. An explicit weight array over the
existing posts maps every sampled rank to a real post.

**Revisit if:** The number of posts grows so large that an explicit
weight array becomes a memory or speed problem. Truncating or
rejecting out-of-range draws would then be the next option.

---

## Scaling a Post's Own Sampling Weight to Model a Burst

**Date:** 2026-11-14

**Rejected alternative:** Modeling a burst as a multiplier
(`burst_intensity`) on the burst post's Zipf sampling weight.

**Why it was considered:** The smallest change: one multiplication in the
sampling weights while a burst is active.

**Why it was rejected:** Burst candidates are idle posts, almost all
low-ranked, with a tiny base weight, so the multiplier adds almost
nothing. Measured with a multiplier of 5, a burst added 1.5 to 1.6
requests (0.18% of the traffic), and the size depends on the post's rank,
the number of posts and the Zipf parameter. A share of traffic is
independent of all three.

**Revisit if:** An experiment needs a burst defined relative to the post's
normal rate (for example ten times its usual traffic) instead of an
absolute share.

---

## Wall-Clock Time for Burst Scheduling

**Date:** 2026-11-14

**Rejected alternative:** Deciding burst eligibility and burst windows from
the measured time (`time.monotonic()`) while sending.

**Why it was considered:** It is the natural choice for a sender that
paces requests in real time.

**Why it was rejected:** Eligibility depended on timing jitter, so two runs
with the same seed and database differed: the same key at 8,997 of 9,000
positions, with one burst on a neighbouring post. Simulated time (request
index divided by the request rate) removes the dependence, and the
paced sender still follows the plan: in a real run the burst start times
matched the planned offsets to the millisecond.

**Revisit if:** The generator has to react to the service's real behavior
(closed-loop load) instead of following a fixed schedule.

---

## Recording a Trace and Replaying It for Identical Bursty Traffic

**Date:** 2026-11-14

**Rejected alternative:** Keeping the wall-clock burst logic and recording
each run's request sequence to replay it against every policy.

**Why it was considered:** It would have fixed the reproducibility problem
without changing how bursts are generated.

**Why it was rejected:** It needs trace files to be stored and shared, and a
deterministic plan gives the same guarantee with only a seed, a config
and a hash (`plan_sha256`).

**Revisit if:** The workload has to come from something a seed cannot
reproduce, such as a real production trace.

---

## Pinning the CPU Power Profile for Benchmarks

**Date:** 2026-11-15

**Rejected alternative:** Switching the machine's power profile or CPU
governor to `performance` to remove the slowdown after idle gaps.

**Why it was considered:** The same call was slower when calls were
spaced, which pointed to CPU power management, and the setting is
reversible.

**Why it was rejected:** It depends on the hardware and on a system setting
that has to be kept the same for every run and recorded, and it would not
remove the cost that actually dominated: building a DataFrame for every
request. Reducing the work per call helps on any machine.

**Revisit if:** The decision time comes close to the timeout again after
the lighter scoring path, or an evaluation needs timing that is stable
across machines, in which case the profile should be fixed and recorded
with the run.

---

## A Fixed 300-Second Look-Ahead Window as the Training Label

**Date:** 2026-11-17

**Rejected alternative:** Labeling a request 1 when the same key is requested
again within 300 s.

**Why it was considered:** It was the first label and needs no cache
capacity.

**Why it was rejected:** About 96% of rows were positive, so the label said
nothing about whether caching pays off in a cache of 250 keys. In an offline
simulation the resulting model admitted 73% of misses and beat plain LRU by
2.9 points, against about 11 for the capacity-aware label. A 300 s window
also made an exact embargo impossible on a 600 s workload.

**Revisit if:** The cache becomes large enough that its turnover time is
comparable to the window, or if a different capacity-independent target is
needed.

---

## A Time Window Derived From the Capacity as the Label

**Date:** 2026-11-17

**Rejected alternative:** Using a short window, for example 5 s (roughly the
time an LRU cache of 250 keys takes to turn over at this request rate and
miss ratio), as the label.

**Why it was considered:** It is simple and needs no new code, only
`--window-sec`.

**Why it was rejected:** It gave the same hit rate as the exact criterion in the
offline study, but the right window depends on the request rate and the miss
ratio, which change between workloads. The exact criterion needs only the
capacity.

**Revisit if:** The criterion becomes too slow on very long logs, or a time
bound is needed for another reason.

---

## Choosing the Admission Threshold by Reject-Class F1

**Date:** 2026-11-17

**Rejected alternative:** Keeping the rule of choosing the threshold that
maximizes F1 of the minority class on the validation set.

**Why it was considered:** It was the existing method and needs no cache
simulation.

**Why it was rejected:** With the capacity-aware label it picks a probability
threshold of about 0.59, which admits almost nothing and gave a hit rate of
42.4%, below plain LRU (43.0%). Only thresholds around 0.2 to 0.3 work, so
the threshold is chosen by simulated hit rate.

**Revisit if:** The simulation becomes too slow, or a metric is found that
tracks hit rate across thresholds.

---

## A Flat Five-Minute TTL for Admitted Keys

**Date:** 2026-11-17

**Rejected alternative:** Keeping `ttl_ms` at five minutes for every
admitted key.

**Why it was considered:** It was the starting value and the manager already
supports TTL.

**Why it was rejected:** It is not model-driven and adds a second variable to
every comparison: hot keys expired after five minutes and cost an extra miss
each. Admitted keys now never expire.

**Revisit if:** A TTL model is trained (`label_ttl_sec` is still written for
that), or a staleness requirement appears.

---

## Fitting Only on Requests That Miss

**Date:** 2026-11-17

**Rejected alternative:** Training only on requests that miss under LRU,
since the model is only queried on misses.

**Why it was considered:** It matches what the model sees in serving.

**Why it was rejected:** In the offline study it changed the hit rate by less
than 0.2 points, and it would need the dataset to carry hit or miss
information or a simulation inside the pipeline.

**Revisit if:** A different workload makes the two distributions differ much
more than in this study.
