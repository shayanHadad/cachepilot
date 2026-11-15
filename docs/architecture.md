# CachePilot — Architecture Decisions

This document records the important design decisions of the project
and the reasoning behind each one, for anyone maintaining or
extending the system.

---

## JSONL Logging: Why, and Its Limitations

### Decision

Raw request logs (`internal/logger`) are written as a plain-text
JSON Lines (`.jsonl`) file — not a separate database, not a message
queue, and not a centralized logging system.
Writing is asynchronous and non-blocking (using a drop-on-full
strategy instead of blocking the main response path when the
internal buffer fills up).
Each record's timestamp is Unix milliseconds (`int64`), the same
convention used for post timestamps (see "Post serialization" below),
so Go and Python read it identically.

### Why this is the right choice for this project

1. **Simplicity and transparency** — a human-readable text file that
   works with simple tools (`cat`, `less`, direct processing with
   pandas), with no need to stand up extra infrastructure.
2. **Full decoupling of producer and consumer** — the Go service has
   no awareness of the final consumer (the data pipeline / ML model
   training); it just records. This decoupling means the downstream
   processing pipeline can be completely changed later without
   touching the main service.
3. **Fits the batch/offline training architecture** — since model
   training happens offline and periodically (not via online
   learning), the model doesn't need real-time data; reading the file
   periodically is entirely sufficient.

### Limitations in a real production system

This decision is appropriate for the scale of this project, but it
has a few known limitations that should be accepted knowingly:

1. **No strong durability** — if the service crashes mid-write, a
   partial JSON line may be left in the file. Production systems
   typically use a message queue (Kafka, Redis Streams) with stronger
   durability guarantees.
2. **Limited scalability** — a single file on a single disk can
   itself become a bottleneck under very high request volume. A
   scalable solution usually involves file rotation plus centralized
   collection (e.g. Kafka → S3, or an ELK stack).
3. **No central schema enforcement** — since raw JSON is written
   directly, changing the `LogEntry` format in the future means old
   and new files have different formats. Production systems use a
   schema registry (e.g. Avro/Protobuf with schema versioning) to
   handle this.
4. **Data loss under heavy load** — the drop-on-full strategy means
   that under very heavy load, some log records are lost (rather than
   blocking). This is acceptable for analytics/model-training metrics
   (and was a deliberate choice to keep the main response path fast),
   but would never be acceptable for, say, a financial transaction
   log. The `Logger.Dropped()` counter exists specifically so this
   rate can be measured and reported rather than hidden.

   > **Update:** nothing reads `Dropped()` or `WriteErrors()` yet:
   > `main.go` closes the logger without reporting either, so a run
   > that lost records would look the same as a clean one. In a check
   > of two 9,000-request runs the live log held all 18,000 records, so
   > nothing was dropped there; the shorter per-run snapshots are
   > explained in "Workload Generator: Bounded Zipf Sampling, Burst
   > Toggle and Per-Run Output".

### Conclusion

This decision was made knowingly, with awareness of its trade-offs,
not out of unfamiliarity with production-grade solutions. For the
goal of this project (comparing cache eviction policies, not building
an industrial logging system), the simplicity of JSONL is worth more
than the scalability/durability of a message queue.

## Post serialization: typed conversion instead of direct map marshaling

**Decision:** When reading a post document from MongoDB, decode it into
a typed struct that mirrors the BSON shape, then explicitly convert
that into a second struct built from plain JSON-friendly types before
marshaling. Do not decode into a generic map and marshal that map
directly.

**Why:**

Decoding into a generic map (`map[string]interface{}`) and marshaling
it directly looks correct but is not: MongoDB's driver types for `_id`
and date fields implement their own `MarshalJSON`, which produces
MongoDB's Extended JSON format instead of plain JSON — e.g. an object
id becomes `{"$oid": "..."}` rather than a plain hex string, and a
timestamp becomes `{"$date": <ms>}` rather than a plain number. Since
both types satisfy Go's `json.Marshaler` interface, the standard
library defers to their custom encoding with no error or warning, so
the generic-map approach fails silently — the bug only surfaces
downstream, when a consumer expects a scalar field and receives a
nested object instead.

The fix is a two-struct pattern: one struct decodes the raw BSON
(using the driver's real types), a second struct holds only plain Go
types (`string`, `int64`, etc.), and an explicit field-by-field
conversion sits between them. This guarantees the JSON that actually
gets cached and logged uses plain, predictable types, regardless of
what the underlying driver's default JSON encoding would have done.

**Timestamp representation:** post timestamps are serialized as
Unix milliseconds (`int64`), not an RFC3339 string, for consistency
with the existing logging timestamp convention — see the JSONL
logging entry above. This avoids ambiguity across the Go/Python
process boundary and keeps timestamp handling uniform across every
component that reads these values (cache layer, offline data
pipeline, feature engineering).

**Trade-off:** this pattern requires keeping two structs in sync
whenever the document schema changes (one field addition means
touching both the BSON-decode struct and the JSON-output struct). This
is an accepted cost in exchange for the serialized output being
explicit and independent of any driver-level default encoding.

---

## Deriving `query_type` at the Serialization Boundary

### Decision

`query_type` ("text_post" vs "media_post") is computed once, inside
the MongoDB read path (`internal/store`), and included directly in
the JSON that gets cached, logged, and passed to the ML decision
service — rather than being recomputed independently by each
consumer.

### Why

`query_type` is a derived field, not something stored in MongoDB
(see the domain model decision: it comes from `media_size_kb == 0`).
Multiple components need it — the request logger, the ML feature
set, potentially the offline data pipeline — and if each computed it
separately from raw post data, a future change to the classification
rule would require updating every call site in lockstep, with no
compiler error if one were missed. Computing it once at the point
where the raw MongoDB document is first read, and carrying it as a
plain field in the serialized JSON from then on, means every
downstream consumer reads the same value instead of re-deriving it.

---

## Per-Key Access Tracking (`internal/features`)

### Decision

A dedicated package maintains a short (five-minute) sliding window of
access timestamps per key, decoupled from both the cache and the ML
transport layer, and exposes a single `Observe(key, timestamp)` call
that returns the current frequency/recency/inter-arrival statistics
for that key.

### Why

The ML decision service needs time-windowed features
(`frequency_1min`, `frequency_5min`, `recency_sec`,
`inter_arrival_avg`) on every cache-miss. Computing these requires
retaining a short history of recent accesses per key somewhere — this
wasn't an explicit part of the original module list and was
identified as a gap while designing the cache manager.

Two design choices are worth calling out:

- **A single `Observe` call instead of separate "record" and
  "compute" calls.** Splitting them invites a subtle ordering bug:
  if the current access is recorded before its own statistics are
  computed, "recency since last access" becomes ~0 on every call,
  silently. `Observe` computes from history _before_ appending the
  current timestamp, making the correct ordering the only one
  possible.
- **Fixed, non-configurable retention windows.** The one- and
  five-minute windows are hardcoded constants rather than
  constructor parameters, so a future caller can't accidentally pass
  a retention shorter than five minutes and silently undercount
  `frequency_5min`.

Cleanup of stale per-key history runs on an independent periodic
timer (not tied to cache eviction), keeping this package fully
decoupled from the cache's own lifecycle.

---

## TTL Bookkeeping and Dual Hit/Miss Counters in the Cache Manager

### Decision

The `Cache` interface (LRU/LFU) has no concept of TTL and never will
— TTL enforcement for the ML-driven policy is handled entirely
inside the cache manager, via its own `key -> expiry time` map,
populated only when the active policy is "ml". Separately, the
manager keeps its own hit/miss counters rather than relying solely on
the underlying cache's counters.

### Why

LRU and LFU are meant to be "pure" baselines with no TTL behavior —
adding a TTL parameter to `Cache.Put` that two of three
implementations would simply ignore was rejected as unnecessary
interface pollution. Instead, the manager treats a cache hit as
"expired" against its own bookkeeping and falls back to a MongoDB
read, without requiring any change to the underlying cache
implementations.

This has one direct consequence for evaluation-metric accuracy: the
underlying cache's own hit/miss counters have no notion of TTL, so
under the "ml" policy they can report a hit for an entry the manager
is about to treat as logically stale. Reading hit rate directly from
the underlying cache would overstate the ML policy's effectiveness.
The manager therefore keeps independent, TTL-aware hit/miss counters
and exposes them via its own `Stats()`, while still deferring to the
underlying cache for the eviction count (a purely capacity-driven
concept with no TTL involvement). For the lru/lfu policies, the
manager's counters and the underlying cache's counters should always
match exactly — a free consistency check during baseline experiments.

### Known limitation

An expired-but-not-yet-overwritten entry can continue occupying a
capacity slot in the underlying cache until the next write to that
key or a natural eviction reclaims it. Fixing this fully would mean
adding explicit deletion to the `Cache` interface and wiring TTL
through every policy implementation — a larger change than
currently justified. Revisit if evaluation runs show this skewing
capacity-pressure results under the "ml" policy.

> **Update:** `Delete(key)` has since been added to the `Cache`
> interface (see "Expired Entries Must Be Removed From the Cache, Not
> Just From the Expiry Map"). Expired entries are now removed on access
> and by the periodic sweep, so a stale entry holds a capacity slot for
> at most one sweep interval. The interface still has no TTL parameter.

---

## Narrowed Scope of `ml_decision.go`; gRPC Client Design

> **Superseded in part:** `ml_decision.go` has since been removed and its
> conversion functions now live in `internal/mlclient`; see "Wire-Format
> Conversion Lives Inside `mlclient`" below. The client design described
> here (no TLS, lazy connect, thin client) still holds.

### Decision

`internal/cache/ml_decision.go` no longer holds a standalone decision
struct — that responsibility moved into `Features`/`Decider` in
`manager.go` once those types were defined directly in the `cache`
package. What remains in `ml_decision.go` is narrower but still
useful: two conversion functions (`ToDecisionRequest`,
`FromDecisionResponse`) that translate between this package's own
types and the generated gRPC wire types. `internal/mlclient` calls
only these two functions and never touches the wire types' fields
directly.

The gRPC client itself (`internal/mlclient/grpc_client.go`) is
intentionally thin: it owns the connection and the RPC call, and
nothing else. It connects without TLS (see the corresponding entry in
`rejected-alternatives.md`) and uses the current lazy-connect client
constructor, meaning a successful client construction does not
guarantee the ML service is reachable — the first real connection
attempt happens on the first RPC call, so "service unavailable"
surfaces there, not at startup.

### Why

Keeping the wire-format conversion logic in one file
(`ml_decision.go`), separate from both the transport code
(`grpc_client.go`) and the orchestration code (`manager.go`), means a
future change to the `.proto` contract (a renamed or added field)
only requires touching one file. It also keeps `manager.go` and
`grpc_client.go` free of any direct dependency on the generated
`decisionpb` types outside that single conversion boundary.

---

## Interface Fix: `Decider.Decide` Also Takes the Key

### Decision

`cache.Decider.Decide` takes `(ctx, key, features)`, not just
`(ctx, features)`.

### Why

The `DecisionRequest` wire message includes the post's key, purely so
the ML service can log/trace a decision back to a specific key on its
own side — the model itself never uses it as an input feature. This
was missed when `Decider` was first drafted (which only threaded
`Features` through) and caught while wiring up the real gRPC client,
which had no way to populate that field without it. Fixed by adding
`key` as an explicit parameter to `Decide`, rather than folding it
into `Features`, to keep `Features` meaning exactly one thing: the
model's actual inputs.

---

## The "ml" Policy's Underlying Storage

### Decision

When `cache.policy` is set to `"ml"`, the service still builds an
`LRU` cache as the concrete `Cache` implementation underneath
`Manager` — there's no separate "ML-native" storage structure.

### Why

`Manager` only decides two things for the ML policy: whether to admit
a value at all, and how long it should live (TTL). Neither of those
is a storage mechanism — something still has to physically hold the
cached bytes, and that something still has a fixed capacity that can
fill up. When it does, an eviction has to happen, and LRU (evict
whatever was least recently touched) is a reasonable default choice
for that fallback, rather than something arbitrary like evicting a
random key.

### Why this matters for the evaluation

It's worth being explicit about this rather than letting it stay an
implementation detail: the "ml" policy is not "ML all the way down".
It's LRU eviction with an ML-driven admission/TTL layer sitting on
top of it. When comparing "ml" against the "lru" baseline in
evaluation, the underlying eviction mechanics are actually identical
in both cases — the only variable being measured is whether the
admission/TTL layer improves on always-admit-no-TTL. This is a
meaningful nuance to state plainly rather than let a reader assume
the ML policy replaced LRU entirely.

---

## Periodic Log Flushing

### Problem found during manual testing

The logger originally only flushed its buffered writer inside
`Close()`. That meant `data/raw_logs/service.jsonl` stayed empty for
the entire time the service was running — data only reached disk on
a clean shutdown. A crash (as opposed to a graceful Ctrl+C) would
have lost every log entry written since the process started, not
just whatever was in the buffer at that moment.

### Fix

The writer goroutine now also flushes on a 1-second ticker,
independent of `Close()`. Since `bufio.Writer` isn't safe for
concurrent use, the periodic flush had to happen on the _same_
goroutine that does all the writing — this ruled out simply spinning
up a second goroutine that calls `Flush()` on its own timer, since
that would race with in-progress writes. Instead, the writer
goroutine's loop now uses `select` over both the entries channel and
the ticker, rather than a plain `for range` over the channel alone.

### What this does and doesn't fix

This bounds the maximum data loss on a crash to roughly one second's
worth of log entries, down from "everything since the process
started." It does not make logging fully crash-safe — anything
written since the last flush is still only in memory until the next
tick. For this project's purposes (training/evaluation data, not a
transactional log), that's considered an acceptable trade-off,
consistent with the drop-on-full policy documented earlier in this
file.

---

## Config Paths Resolve Relative to the Config File, Not the Working Directory

### Problem found during manual testing

`logging.path` in `config.yaml` was a relative path
(`data/raw_logs/service.jsonl`). Relative paths are resolved against
the process's current working directory — so running the service
from inside `go-cache-service/` put the log file at
`go-cache-service/data/raw_logs/`, while running it from the repo
root would have put it somewhere else entirely. This silently broke
the assumption (used throughout the rest of the project, e.g. the
data pipeline) that logs always live at `<repo root>/data/raw_logs/`.

### Fix

`config.Load()` now resolves any relative path fields (currently just
`logging.path`) against the directory containing the config file
itself, not against the working directory the process happens to be
started from. `config.yaml` was updated accordingly
(`../data/raw_logs/service.jsonl`, since the config file lives one
level down inside `go-cache-service/`).

### Why this matters beyond just fixing the immediate bug

This removes an entire class of "works on my machine" failure: the
log file's location no longer depends on which directory a command
happens to be run from — a shell script, an IDE run configuration, or
a future Docker `WORKDIR` can all differ without breaking anything,
as long as the same config file path is passed to `config.Load()`.

> **Limitation:** `main.go` passes the literal path `config.yaml`, which
> is resolved against the working directory, so the service still has to
> be started from `go-cache-service/`. The ML service applies the same
> rule to `model_path`, resolved against its own `config.yaml`. The
> scripts in `data-pipeline/` and `ml-service/model/train.py` use
> defaults relative to the working directory and must be run from their
> own directories (see "Data Pipeline Stages Run Alone or Chained").

---

## Burst Candidates Must Be Idle First

### Problem found during manual testing

The workload generator's burst injection originally picked any
random post to burst, with no regard for whether that post was
already naturally hot. In practice this meant a "burst" frequently
landed on a post that was already being requested often — which has
almost no measurable effect, since a post that's already hot wasn't
at risk of eviction to begin with. A manual A/B test (normal vs.
burst workload) showed close to zero difference in hit rate or
latency, which is what surfaced this.

### Fix

`BurstState` now tracks each post's last-access time. A post only
becomes eligible to start a new burst once it's gone at least
`burst_min_idle_sec` seconds without being requested (or has never
been requested at all). This is what actually models the scenario
the project cares about: a previously cold post suddenly going
viral — not an already-popular post getting a bit more popular.

### Why this is a general rule, not a one-off fix

`burst_min_idle_sec` is a config parameter, not a hardcoded
threshold, and the eligibility check works the same way regardless of
dataset size, Zipfian parameter, or request rate. The underlying rule
("only idle keys can burst") is what matters methodologically, not
any specific value chosen for a particular test run.

> **Update:** `BurstState` no longer exists. The same rule is applied in
> `workload_plan.build_plan` on simulated time (see "Burst Traffic Is
> Planned as a Share of Requests"); `burst_min_idle_sec` keeps its
> meaning.

---

## Logging Which Policy Actually Produced Each Response

### Problem

`LogEntry` originally only recorded hit/miss, not _why_ — for the
"ml" policy specifically, there was no way to tell from the log alone
whether a given response was actually decided by the ML service, or
by the fallback path (when the ML service is slow or unavailable).
Since the fallback behaves like a working response (the request still
succeeds), this distinction was invisible from the outside without
watching the service's stdout in real time.

### Decision

Added a `Source` field to `LogEntry`. Its value depends on how the
response was produced:

- `"cache-hit"` for any cache hit, regardless of policy — no
  admission decision was made, the value was already sitting in the
  cache.
- The policy name (`"lru"` or `"lfu"`) for a miss under a baseline
  policy — there's no separate decision-maker to distinguish there.
- The decider's own reported source for a miss under the "ml"
  policy — either whatever the ML service returns (`"heuristic-v1"`
  today, a model identifier later) or `"fallback-lru"` if the gRPC
  call failed or timed out.

`Manager.admit()` was changed to return this source string instead of
nothing, so `Manager.Get()` can pass it straight into the log entry.

### Why this matters for evaluation

Average hit rate and latency alone can't answer "how often did the ML
service actually make the call, versus falling back?" — which
directly affects how any measured improvement should be interpreted.
With `Source` in every log line, that split can be computed directly
from `data/raw_logs/service.jsonl` without any additional
instrumentation.

> **Update:** in `model` mode the decider's source is the `model_name`
> from the model's metadata (`lightgbm-` plus 8 hex characters), and
> `heuristic-v1` in `heuristic` mode; see "Model Identity Is Stored in
> Its Metadata and Reported as the Decision Source". The field was also
> not being written to the JSONL file until a later fix; see "The
> `source` Field Was Not Written to the JSONL Log".

---

## Splitting the ML Service's gRPC Server from Its Decision Logic

### Decision

`ml-service/server/grpc_server.py` only translates between the gRPC
wire format and a single function call, `model.inference.decide()`.
It has no admission-rule logic of its own. Today, `decide()` is a
simple heuristic (admit if requested at least once in the last
minute, flat 5-minute TTL) — not a trained model yet, since the
training data pipeline doesn't exist yet.

### Why

This split means the eventual trained model can replace the
heuristic by changing only `inference.py` — `grpc_server.py` doesn't
need to change at all, since it was never written to know or care
what's behind `decide()`. This mirrors the same interface-first
approach used on the Go side (`cache.Decider`): the transport layer
and the decision logic are separate concerns, developed and testable
independently.

### Why start with a real heuristic instead of a stub

The heuristic isn't a placeholder that always returns the same
answer — it's a real (if simple) admission rule. This let the full
Go-to-ML gRPC path be tested end to end immediately, without waiting
for `data-pipeline/` and `model/train.py` to exist first, while still
producing meaningful (not arbitrary) cache behavior in the meantime.

> **Update:** this entry describes the original state. The training
> pipeline now exists and `decide()` serves the trained LightGBM model
> by default, with the heuristic kept as an explicit `decision_mode`
> (see "The ML Service Fails Fast Instead of Falling Back Silently").
> The split between `grpc_server.py` and `inference.py` is unchanged.

---

## Fixing Early Stopping for the Admit Model

### Problem

The first working version of `model/train.py` collapsed to always
predicting "admit," never predicting the minority "reject" class at
all (0% precision/recall on `reject`) — despite `scale_pos_weight`
being computed and set correctly from the training set's class
balance. Inspecting the training log showed early stopping
triggering at boosting iteration 1: the model was stopped after a
single tree and never got a real chance to learn anything. Predicted
probabilities on the test set were narrowly ranged (0.68–0.97) and
didn't separate true-reject from true-admit examples at all.

### Root cause

Early stopping was watching `binary_logloss` alone, with only 20
rounds of patience. On this heavily imbalanced target, logloss can
behave noisily in the first few rounds, and the combination of a
single noisy metric with low patience caused the trainer to give up
immediately rather than let the model actually converge.

### Fix

Added `auc` as a second tracked metric alongside `binary_logloss`,
raised early-stopping patience to 50 rounds, and logged every
boosting round (instead of every 50) to make the metric trend
visible. With this change, training ran for 83 rounds and selected
iteration 33 as the best, with validation AUC climbing from 0.881 to
0.894 before gradually overfitting — the kind of curve a healthy
training run is expected to show, confirming the original 1-round
stop was the actual bug, not a sign the task was unlearnable.

A reference result from one full run (60,000 requests, zipf_param=0.9,
5000 seeded posts, cache capacity 250): test ROC AUC 0.899, reject
average precision 0.183 (vs. a random baseline of 0.039 — about 4.7x
better than chance). See `docs/experiment-log.md` for the full,
reproducible record of this run.

### Side finding: not all features are equally useful

A single-feature AUC check (scoring each feature alone against
`label_admit`) showed `frequency_5min` alone (AUC 0.905) actually
outperforms the full multi-feature model (AUC 0.894). `query_type`
and `payload_size_kb` scored close to random (AUC ~0.51) on their
own, and `query_type` has zero LightGBM feature importance (gain =
0.0) in the trained model. This isn't a bug — it's a legitimate
finding worth keeping for the evaluation writeup, and a candidate for
a future feature-selection experiment (does dropping the near-useless
features change anything, for better or worse).

> **Update:** `train.py` states the root cause more precisely: validation
> logloss is computed without class weights while the training objective
> is weighted by `scale_pos_weight`, so the two disagree and logloss
> stops improving at round 1. AUC does not depend on weighting, which is
> why it is the first metric and the only one that can stop training.

## Expired Entries Must Be Removed From the Cache, Not Just From the Expiry Map

### Problem

Under the "ml" policy, `Manager` tracked per-key expiry deadlines in a
separate map, while the values themselves lived in the underlying LRU,
which has no notion of TTL. The periodic cleanup only dropped deadlines
that had passed. Once a deadline was gone, `isExpired` reported the key
as live, so the stale value still in the LRU was served as a hit. A key
could therefore be a logical miss before the first cleanup sweep and a
hit after it, which inflated the hit rate of the "ml" policy relative to
lru/lfu.

### Decision

Add `Delete(key)` to the `Cache` interface. `Manager` now removes the
value together with its deadline, both when a request finds an expired
entry and when the periodic sweep drops it. Entries admitted without a
TTL (for example the `fallback-lru` path) also clear any older deadline
for that key.

### Why

Expiry and presence of a value must not be tracked independently:
removing only the bookkeeping silently turns "expired" into "valid".
The failure only appears when expiry and cleanup interleave, so it was
reproduced with a test (`TestExpiredEntryIsNotServedAsHitAfterCleanup`)
before the fix.

### Trade-offs / Limitations

`Delete` is not counted as an eviction, so `Stats().Evictions` still
means capacity-driven removals only. A concurrent re-admit can still
lose its fresh value to a sweep in a narrow window; the cost is one
extra miss, not a stale hit.

## The ML Service Fails Fast Instead of Falling Back Silently

### Problem

If the ML service answered with a different decider whenever the model
could not be loaded, the results labeled "ml" would quietly become a mix
of deciders, and nothing in the output would show it. The Go service
already has its own fallback (`fallback-lru`) for an ML service that is
unreachable or returns an error.

### Decision

`ml-service/config.yaml` has an explicit `decision_mode`: `model`
(default) or `heuristic`. In `model` mode the service refuses to start
when the model file or its metadata is missing, when the metadata lacks
required keys, when the model file's sha256 does not match the one
recorded in the metadata, or when the feature names in the model or its
metadata differ from the columns the serving code builds. A request that
cannot be scored, such as an unseen `query_type`, is answered with
`INVALID_ARGUMENT`, which the Go client handles like any other error.

### Why

Each layer has one fallback path. Python either decides with the
configured decider or fails visibly, and Go decides what happens when it
fails. `heuristic` mode stays available as an explicit baseline for
evaluation, not as a safety net.

### Trade-offs / Limitations

Trained model files are not committed, so a fresh checkout has to run
`ml-service/model/train.py` before the service will start. The checks run
at startup only.

> **Update:** a model file that does not store the `query_type` categories
> is also refused at startup, because single requests are scored from the
> category codes (see "Single Requests Are Scored From a NumPy Row, Not a
> DataFrame"). Earlier, a missing list silently skipped the unseen
> `query_type` check.

## Model Identity Is Stored in Its Metadata and Reported as the Decision Source

### Problem

A hardcoded source name cannot tell two trained models apart, so a logged
decision could not be tied to the model that produced it.

### Decision

`train.py` writes `model_name` (`lightgbm-` plus the first 8 hex characters
of the model file's sha256), `model_sha256`, `trained_at` and
`code_version` (git commit, with a `+dirty` suffix for uncommitted changes)
into `model_meta.json`. The serving side verifies the hash and returns
`model_name` as `DecisionResponse.source`, which ends up in
`LogEntry.Source`.

### Why

The name comes from the file's content, so it cannot drift from the model
it describes, and evaluation can count decisions per model.

### Trade-offs / Limitations

A model trained from an uncommitted tree records a `+dirty` code version.
Commit before training for any logged experiment. The check covers the
whole working tree, including untracked files and documentation edits,
not only the ML code.

## Single-Request Scoring Is Checked Against Batch Scoring

### Problem

The model is trained on a batch DataFrame, but the service scores one
request at a time. Column order or `query_type` category handling could
differ between the two paths and produce wrong scores without any error.

### Decision

Serving reuses `prepare_features` from `features/feature_engineering.py`.
`ml-service/model/check_inference.py` scores every row of the test set
through the single-request path and compares probabilities and decisions
with batch scoring, checks that an unseen `query_type` is rejected, and
reports in-process decision latency.

### Why

Parity on a real dataset covers both `query_type` values and the column
order in one check, and it can be re-run after every retraining.

### Trade-offs / Limitations

The latency figure excludes gRPC and network time, while the Go side's
8 ms ML timeout includes them.

> **Update:** serving no longer calls `prepare_features`; single requests
> are scored from a one-row numpy array (see "Single Requests Are Scored
> From a NumPy Row, Not a DataFrame"). `check_inference.py` is unchanged
> and still compares that path with batch scoring, which is what protects
> it. After the change it reported a maximum probability difference of 0
> on 5,999 test rows and a median in-process decision time of 0.082 ms.

---

## Wire-Format Conversion Lives Inside `mlclient`

### Decision

`internal/cache/ml_decision.go` was removed. The conversion between the
`cache` package's own types (`Features`, `types.CacheDecision`) and the
generated `decisionpb` messages now lives in
`internal/mlclient/grpc_client.go`, as two unexported functions,
`toDecisionRequest` and `fromDecisionResponse`. `mlclient.Client`
implements `cache.Decider`, so the `cache` package never imports
`decisionpb`.

### Why

The wire types are only needed where the RPC happens. Keeping the
conversion next to the call means a `.proto` change touches one
package, and the cache manager stays independent of the transport. This
supersedes the `ml_decision.go` part of "Narrowed Scope of
`ml_decision.go`; gRPC Client Design"; the client itself is unchanged
(no TLS, lazy connect, no logic beyond the RPC).

---

## The Cache Manager Depends on a `PostFetcher` Interface

### Decision

`cache.Manager` loads posts through a small `PostFetcher` interface
declared in the `cache` package (`GetPost(ctx, id) ([]byte, error)`)
instead of holding a `*store.Store`. The Mongo store satisfies it, and
`manager.go` no longer imports `internal/store`.

### Why

The manager's admission and TTL logic can be tested with an in-memory
fake, without a running MongoDB; `manager_ttl_test.go` does this with a
fake post source and a fixed decider. It also follows the pattern already
used for `Decider`: the consuming package defines what it needs, and
the concrete implementations live elsewhere.

### Trade-offs / Limitations

The interface is satisfied implicitly, so a signature change in the
store is caught by the compiler where `main.go` wires the two together,
not next to the interface.

---

## The `source` Field Was Not Written to the JSONL Log

### Problem

`LogEntry` carried a `Source` field and the cache manager filled it in,
but the JSONL logger did not write it to the file. Nothing failed: the
service ran and the logs looked normal. The gap only shows up when a log
is analyzed for the model-versus-fallback breakdown.

### Decision

The logger now writes `source` on every record, and
`TestLoggedLineCarriesEveryLogEntryField` (in
`internal/logger/json_logger_test.go`) checks that every `LogEntry`
field appears in the logged line. Logs recorded before the fix (before
2026-10-13) cannot be used for the model-versus-fallback breakdown.

### Why

A field that exists in a struct but not in the output fails silently. A
quick look at the first lines of a fresh log catches it.

### Trade-offs / Limitations

Older logs cannot be re-analyzed for `source`. Experiment 1 does not
depend on it: the dataset pipeline reads only the key, timestamp and
query type from each record.

---

## Offline Features Mirror the Live Tracker; `payload_size_kb` Comes From MongoDB

### Decision

`data-pipeline/build_dataset.py` replays the raw JSONL logs in
timestamp order and computes `frequency_1min`, `frequency_5min`,
`recency_sec` and `inter_arrival_avg` for each request from that key's
history before the request, with the same windows and zero values as
`internal/features/tracker.go`. `payload_size_kb` is the post's
`media_size_kb`, read from MongoDB in one batch query, not the log's
`response_size`.

### Why

A model trained on features computed differently from the ones it sees
at serving time has a systematic mismatch that no offline metric
reveals. Using only prior history keeps future data out of the
features. `response_size` is the full serialized JSON size and is
always above zero, even for text posts, while the ML service sees
`media_size_kb` (0 for text posts).

### Trade-offs / Limitations

- The Python code is a port of the Go tracker, and no automated test
  compares the two, so they have to be changed together.
- The live tracker uses nanosecond timestamps while the log stores
  milliseconds, so offline `recency_sec` and `inter_arrival_avg` can
  differ from the live values by about a millisecond.
- `media_size_kb` is read when the dataset is built, not when the log
  was recorded. The posts must not be re-seeded in between, and a key
  missing from MongoDB silently gets 0.0 (the script only reports how
  many keys were found).
- Every logged request becomes a training row, hits included. At
  serving time the tracker observes every request too, so the feature
  values match, but the model is only queried on cache misses. Training
  rows therefore over-represent hot keys compared with what the model
  scores in production.
- Malformed log lines (for example a truncated last line after a forced
  kill) are skipped with a warning.

---

## Look-Ahead Labels Are Computed in a Separate Pass, With a Right-Censoring Trim

### Decision

`data-pipeline/labeling.py` adds labels in a separate step from
`build_dataset.py`. `label_admit` is 1 when the same key is requested
again within `label_window_sec` (default 300 s) after the access, and 0
otherwise. `label_ttl_sec` (seconds to the next access, NaN when there
is none) is kept as an optional target but is not used by training.
Rows within the window of the log's end are dropped, and the step fails
with an explicit error if no rows remain.

### Why

Features look backward only, to match what is available at inference
time; labels look forward. Separate passes make it hard to leak a
look-ahead value into a feature. Near the end of the log, a missing
next access may mean the recording stopped, not that the key was never
requested again (right-censoring). Keeping those rows would
under-label the tail, and because the time split puts the tail of the
log into the test set, the bias would land only in test.

### Trade-offs / Limitations

- The trim discards one window of data: Experiment 1 (600 s of traffic,
  300 s window) kept 29,993 of 59,993 rows. The workload must be
  several times longer than the window.
- The label means "requested again within the window", not "caching it
  would have produced a hit". It ignores cache capacity: a key that
  comes back after many other distinct keys is labeled admit even if it
  would have been evicted first. With 5,000 posts at 100 requests per
  second, about 96% of rows are admit, which leaves the model very
  little to separate.

---

## Time-Based Splits Without a Gap Between Train and Test

### Decision

`data-pipeline/time_split.py` sorts the labeled rows by timestamp and
puts the first `train_fraction` (default 0.8) in the training set and
the rest in the test set. `train.py` carves its validation set the same
way: the last 15% of the training rows by time. There is no random
split anywhere.

### Why

A random split would train on rows that happened after some of the
test rows, which never happens at serving time, so the test score would
overstate performance on unseen future traffic.

### Trade-offs / Limitations

Both splits are by row count with no gap between the parts.
`label_admit` looks `label_window_sec` (300 s) into the future, so the
label of a late training row can depend on requests that fall inside
the test period (and the same holds between fit and validation). In
Experiment 1 the labeled rows cover only the first 300 s of the log, so
the test period is the last minute of that span and training labels
reach into it. The reported test and validation metrics may therefore
be slightly optimistic; the size of the effect has not been measured.
Dropping training rows closer than `label_window_sec` to each split
point (an embargo) would remove the overlap.

---

## Admit Model Training Setup

### Decision

`ml-service/model/train.py` trains a LightGBM binary classifier on
`label_admit`. The positive class is admit, so "reject" is the minority
class.

- `scale_pos_weight` is negatives over positives in the fit set,
  computed at training time (0.038 in Experiment 1), which down-weights
  the majority admit class.
- Early stopping watches validation AUC only (patience 50, up to 1000
  rounds); `binary_logloss` is logged but does not stop training.
- The reject-score threshold (`1 - p_admit`) is chosen on the
  validation set to maximize reject-class F1, not fixed at 0.5. It is
  saved in `model_meta.json`, and `inference.py` applies the same rule.
- The test set is used once, for the final report, at both the default
  and the tuned threshold.
- Hyperparameters use the CLI defaults (learning rate 0.05, 15 leaves,
  `min_child_samples` 50); the seed is fixed at 42 in code. No search is
  run.

### Why

The target is heavily imbalanced, so the minority class needs explicit
weighting and a tuned threshold instead of the default cut-off. The
threshold is part of the decision rule, so it travels with the model.

### Trade-offs / Limitations

- The model predicts admission only; TTL is not learned (see "TTL Is a
  Flat Default in Both Decision Modes").
- The threshold was selected on very few validation reject rows (132 in
  Experiment 1) and landed close to 0.5, so it is noisy.
- Validation shares the label-window overlap described in "Time-Based
  Splits Without a Gap Between Train and Test".
- Hyperparameters were not tuned.

---

## TTL Is a Flat Default in Both Decision Modes

### Decision

`inference.py` returns the same fixed TTL, five minutes
(`DEFAULT_TTL_MS`), for every admitted request in both `model` and
`heuristic` mode, and 0 for rejected ones. The model does not predict
TTL, and `label_ttl_sec` is not used. The cache manager sets an expiry
deadline from the TTL at admission; hits do not extend it.

### Why

The first trained model predicts admission only. The five-minute value
is a starting point, not derived from data.

### Trade-offs / Limitations

- Under the "ml" policy an entry is dropped five minutes after
  admission even if the key is still hot, which costs one extra miss
  per hot key per five minutes and can lower the hit rate relative to
  plain LRU. Any measured difference between "ml" and "lru" mixes the
  admission decision with this expiry.
- The project's stated goal is a model-driven TTL. A TTL model or a
  rule based on `inter_arrival_avg` is the natural next step.

---

## Data Pipeline Stages Run Alone or Chained

### Decision

`data-pipeline/` has three stage scripts: `build_dataset.py` (logs to
features), `labeling.py` (features to labels) and `time_split.py`
(labeled rows to train and test sets). Each runs on its own.
`pipeline.py` runs them in order with their default paths and stops at
the first stage that fails. Training (`ml-service/model/train.py`) is a
separate step.

### Why

Separate stages let one be re-run, for example `labeling.py` with a
different `--window-sec`, without recomputing the others. The chained
runner covers the common case.

### Trade-offs / Limitations

- Default paths are relative to the working directory (`../data/...` in
  `data-pipeline/`, `../../data/...` and `artifacts/model.txt` in
  `train.py`), so each script must be run from its own directory.
  `check_inference.py` is the exception: it derives its paths from its
  own location.
- `build_dataset.py` reads every file matching its `--logs` glob and does
  not remove duplicate records, so overlapping log snapshots would be
  counted twice (see "Workload Generator: Bounded Zipf Sampling, Burst
  Toggle and Per-Run Output").

---

## Workload Generator: Bounded Zipf Sampling, Burst Toggle and Per-Run Output

### Decision

`workload-generator/zipfian_generator.py` sends requests to `GET /get`
one at a time, choosing keys among the post ids that exist in MongoDB.

- Popularity follows a bounded Zipf distribution built as an explicit
  weight array (1 / rank^s over the existing posts), not
  `numpy.random.zipf`, whose rank is unbounded and can exceed the
  number of posts.
- Burst injection is off by default and enabled with `--enable-burst`,
  so one generator produces both the stationary and the bursty workload
  needed to compare policies on non-stationary access. While a burst
  is active it takes a share of the traffic (`burst_share`) for
  `burst_duration_sec`; candidates must have been idle for
  `burst_min_idle_sec` (see "Burst Candidates Must Be Idle First" and
  "Burst Traffic Is Planned as a Share of Requests"). Each burst is
  written to `burst_ground_truth.jsonl`.
- Requests are scheduled against absolute times on a monotonic clock,
  so timing does not drift over a long run.
- Each run writes to its own folder, `run_<timestamp>_<normal|burst>`,
  with `results.jsonl`, `burst_ground_truth.jsonl` (burst runs),
  `run_config.json` (every parameter plus the plan and post list
  fingerprints) and a copy of the Go service's log. Parameters are a
  dataclass overridable by CLI flags, not a YAML file.

### Why

The generator is a run-once script re-invoked with different parameters
per experiment, so the command line is the record of what was run, and
a separate folder per run keeps results from overwriting each other.
Bounded weights map every sampled rank to a real post.

### Trade-offs / Limitations

- **Earlier burst design (replaced).** The first design multiplied the
  post's own sampling weight by `burst_intensity`. Measured in two runs
  (5,000 posts, Zipf 0.9, 100 requests per second, intensity 5, 30 s
  bursts, 8 bursts per run), a burst added 1.5 to 1.6 requests on
  average above the key's own base rate (at most 7.5), and only 16 of
  9,000 requests (0.18% of traffic) fell inside a burst window on the
  burst key, which is hard to tell apart from the stationary workload.
  Because eligibility used measured idle time, two runs with the same
  seed and database also differed: the same key at 8,997 of 9,000
  positions, with one of eight bursts on a neighbouring post. "Burst
  Traffic Is Planned as a Share of Requests" replaces this design. Key
  identities still depend on the seeded database: re-seeding creates new
  ids (the run records a fingerprint of the post list).
- **Cumulative log snapshot (measured).** The Go logger appends to its
  file and the generator copies the whole file at the end of a run (a
  copy, not a move, because the service may still be writing). If the
  file is not removed or redirected between runs, each run's
  `service.jsonl` also holds the earlier runs' records: in two
  consecutive runs the second snapshot held all 9,000 records of the
  first run plus its own. Use a separate `CACHEPILOT_LOGGING_PATH`, or
  clear the file, for every run.
- **Snapshot tail (measured).** The copy is made when the last request
  returns, before the logger's buffer is fully flushed, so the last few
  records are missing and the last line can be cut in the middle: the
  first snapshot held 8,994 of 9,000 records and the second ended in a
  truncated line, while the live log held all of them.
  `build_dataset.py` skips a truncated line with a warning. Experiment 1
  was 7 records short (59,993 of 60,000), which is consistent with this.
- Requests are sequential, so a request slower than the scheduled
  interval lowers the achieved rate below `requests_per_sec`. In the
  two runs above, client latency averaged about 5 ms with p99 under 9
  ms, below the 10 ms interval, and 9,000 requests took 90.0 s.

---

## HTTP Surface of the Cache Service

### Decision

The service exposes one endpoint, `GET /get?key=<hex id>`, which returns
the post's JSON. A missing key gives 400, `store.ErrPostNotFound` gives
404 and any other error gives 500 with a generic body. The handler calls
`Manager.Get`, the only place that logs requests.

### Why

Only point lookups by `_id` are supported, so a single endpoint keeps
the surface that benchmarks exercise small.

### Trade-offs / Limitations

- An id that is not a valid hex ObjectID returns 500, not 400 (checked
  with curl; a missing key gives 400 and an unknown valid id gives
  404), because the store reports it as a generic error.
- A request that fails before the cache decision is not logged; the
  workload's `results.jsonl` is the only record of failures. The
  underlying cache still counts such a lookup as a miss while `Manager`
  does not, so the statement that the two counters match under lru and
  lfu holds only when no fetch fails.
- No endpoint exposes `Manager.Stats()` or `Logger.Dropped()`. Hit rate
  is computed from the JSONL log.

---

## Generated Protobuf Stubs Are Committed; the Python Import Is Edited by Hand

### Decision

The stubs generated from `proto/cache_decision.proto` are committed for
both languages: `go-cache-service/internal/mlclient/decisionpb/*.pb.go`
and `ml-service/decisionpb/cache_decision_pb2*.py`. In the generated
Python gRPC module the import was changed by hand to a relative one
(`from . import cache_decision_pb2 as ...`), and
`ml-service/decisionpb/__init__.py` makes the directory a package.
`grpc_server.py` imports the stubs as `decisionpb` after adding
`ml-service/` to `sys.path`.

### Why

Committed stubs mean nobody needs `protoc` to run the services. The
relative import makes the generated module work as part of a package;
the generator emits an absolute import that fails there.

### Trade-offs / Limitations

- Regenerating the Python stubs overwrites the hand-edited import, so the
  edit has to be repeated. The regeneration command in `__init__.py` does
  not mention this step.
- The generated code checks the installed protobuf runtime at import time
  and raises a `VersionError` on an incompatible version. The committed
  stubs record protobuf 5.28.1 in their header, while
  `requirements.txt` pins the runtime to 5.29.3 (same major version, and
  it works). When protobuf is upgraded, the header version should be
  checked and the stubs regenerated.

---

## Python Dependencies Are Pinned for the ML Service Only

### Decision

`ml-service/requirements.txt` pins grpcio 1.68.1, grpcio-tools 1.68.1,
protobuf 5.29.3, pyyaml 6.0.2, lightgbm 4.5.0, scikit-learn 1.5.2,
pandas 2.2.3 and pyarrow 18.1.0. Go dependencies are in `go.mod`
(Go 1.26.1, mongo-driver 1.17.9, grpc 1.83.1, protobuf 1.36.12,
yaml.v3 3.0.1).

### Why

The ML service is the part where a library version changes results (the
model file and the `VersionError` check both depend on it), so it was
pinned first.

### Trade-offs / Limitations

`data-pipeline/`, `workload-generator/` and `scripts/` have no
requirements file. Their docstrings mention `pymongo`, `requests`,
`numpy` and `pandas`, and `build_dataset.py` also needs `pyarrow` to
write parquet. The environment for those steps cannot be recreated from
the repository, and a virtual environment built only from
`ml-service/requirements.txt` lacks `pymongo` and `requests`.

---

## Burst Traffic Is Planned as a Share of Requests

### Problem

Measured on the first burst design (see "Workload Generator: Bounded
Zipf Sampling, Burst Toggle and Per-Run Output"), a burst on a typical
idle post added about 1.5 requests, 0.18% of the traffic, and runs with
the same seed were not exactly identical because burst eligibility
depended on wall-clock timing. Burst mode was therefore hard to tell
apart from the stationary workload and could not be replayed exactly.

### Decision

- The request sequence is built before anything is sent, by
  `workload_plan.build_plan` (numpy only, no I/O, no wall clock).
  `zipfian_generator.py` only sends it.
- A burst takes a fixed share of the traffic: while it is active, each
  request goes to the burst key with probability `burst_share` (default
  0.10). `burst_rate` (default 1/60 per second, previously 0.02) is the
  chance per second that a new burst starts, `burst_duration_sec` (30)
  is its length, and `burst_min_idle_sec` (10) still restricts
  candidates to idle posts. Overlapping bursts take their shares
  independently, in start order. `burst_intensity` was removed, so
  `--burst-intensity` is now rejected.
- Time is simulated as request index divided by `requests_per_sec`, so
  idleness and burst windows come from the plan itself, not from
  measured time.
- Stationary requests and bursts use two separate random streams. With
  bursts off the sequence is the one the generator produced before (a
  unit test compares them), and a burst run is the stationary run with
  some requests replaced by the burst key.
- Each plan has a SHA-256 fingerprint. `run_config.json` records
  `plan_sha256`, `post_ids_sha256` (the ordered post id list) and
  `n_posts`. `results.jsonl` gains `index` and `burst_id`, and
  `burst_ground_truth.jsonl` gains `burst_id`, `rank`, `start_index`,
  `end_index` and `requests` next to the existing `start_ts`, `end_ts`
  and `key`.
- `--dry-run` (with `--dry-run-posts`, default 5,000) prints the plan and
  the burst table on synthetic posts without MongoDB or the service.
  `requests` and `pymongo` are imported only when sending, so a dry run
  and the tests need only numpy. The tests are in
  `test_workload_plan.py` (`python -m unittest -v` from
  `workload-generator/`).

### Why

A share does not depend on the burst post's rank, the number of posts or
the Zipf parameter, so a burst means the same thing across experiments:
about `burst_share × burst_duration_sec × requests_per_sec` requests,
300 with the defaults at 100 requests per second. A deterministic plan
makes runs comparable without recording and replaying a trace, and a
burst run differs from its stationary counterpart only at the replaced
requests.

### Trade-offs / Limitations

- Measured in a real run (18,000 requests at 100 per second, 5,000
  posts, Zipf 0.9, burst rate 0.03): 6 bursts took 1,684 requests
  (9.36% of the traffic), 253 to 308 per burst (8.4% to 10.3% of the
  requests in its window), on posts of rank 304 to 2,907. A dry run on
  synthetic posts printed the same plan hash as the one the real run
  recorded, no request failed, and each burst's start time in the
  ground-truth file matched the planned offset to the millisecond.
- A burst window can be one request slot longer than planned (3,001
  instead of 3,000 slots in two of those six bursts) because of
  floating-point rounding at the end of the window. The result is
  deterministic.
- The `requests` field in `burst_ground_truth.jsonl` is the planned
  count, not a count of responses.
- Bursts are steps: they start and end abruptly, with no ramp or decay.
- The plan depends on the post list. A dry run matches a real run only
  if the database holds the same number of posts, and exact reproduction
  needs the same ordered list (`post_ids_sha256`).
- The measurements in "Workload Generator: Bounded Zipf Sampling, Burst
  Toggle and Per-Run Output" describe the replaced design.

---

## Single Requests Are Scored From a NumPy Row, Not a DataFrame

### Problem

In a first smoke test with policy `ml` (6,000 requests at 100 per second),
33.7% of the misses that needed a model decision (1,111 of 3,296) ended
as `fallback-lru`, because the decision did not return within the 8 ms
timeout. The in-process check had reported about 2.1 ms per decision, so
the live cost was far above the tight-loop figure (see "ML Decision
Latency Depends on the Call Pattern").

### Decision

`inference.admit_probability` builds a one-row numpy array in
`FEATURE_COLUMNS` order and passes `query_type` as its category code (the
index in the category list stored in the model file), instead of building
a pandas DataFrame for every request. At startup the service refuses a
model file that does not store that list.

### Why

A throwaway diagnostic timed the same call back to back (`tight`), every
10 ms (`paced`), and at irregular gaps like cache misses (`sporadic`, 55%
of the 10 ms ticks), both in-process (`local`) and as a gRPC call to the
running service. Median time in ms, and the share of calls over the 8 ms
timeout:

|                | DataFrame p50 | over 8 ms | NumPy p50 | over 8 ms |
| -------------- | ------------- | --------- | --------- | --------- |
| local tight    | 2.22          | 0.0%      | 0.05      | 0.0%      |
| local paced    | 3.95          | 2.3%      | 0.58      | 0.1%      |
| local sporadic | 5.65          | 26.7%     | 0.57      | 0.0%      |
| grpc tight     | 2.75          | 0.0%      | 0.48      | 0.0%      |
| grpc paced     | 4.48          | 17.6%     | 3.20      | 0.0%      |
| grpc sporadic  | 7.09          | 43.3%     | 2.84      | 0.1%      |

Building the frame was almost all of the per-call work. The probabilities
are identical to the DataFrame path and to batch scoring (maximum
difference 0 on 5,999 test rows, no decision differs).

### Trade-offs / Limitations

- The category codes come from the category list in the model file, read
  at startup, so a retrained model brings its own codes.
- `check_inference.py` has to be re-run after every change to the scoring
  code and after retraining; it is the only protection against a wrong
  column order or category code.
- An unseen `query_type` is still rejected with `INVALID_ARGUMENT`.

---

## ML Decision Latency Depends on the Call Pattern

### Problem

The same decision took longer when calls were spaced than when they came
back to back, so a latency figure from a tight loop did not predict live
behavior. In the first smoke test (policy `ml`, 6,000 requests at 100 per
second, 5,000 posts, Zipf 0.9, capacity 250, DataFrame scoring), the
share of model calls that ended as a fallback rose with the gap since the
previous model call: 29.0% for back-to-back calls, 34.5% for a gap of two
requests, 41.6% for three to four and 60.4% for five to nine. Fallbacks
were clustered (71.6% after a fallback, 14.4% after a success) and did
not depend on `query_type` (33.7% for text posts, 33.8% for media
posts). The decision time of the successful calls, estimated as the miss
latency minus the median miss latency under lru, had a median of about
4.9 ms.

### Decision

- The scoring path was made lighter (see "Single Requests Are Scored From
  a NumPy Row, Not a DataFrame").
- The `source` field makes the fallback share measurable in every run, so
  any evaluation run should report it, and should leave out the first
  requests of a run or warm up the gRPC connection first, because the
  first calls fall back while the lazy connection is established.
- The cause of the call-pattern effect was not established.

### Why

After the change, a second smoke test on the same 6,000 keys gave:

|                                            | before (DataFrame)     | after (NumPy)         |
| ------------------------------------------ | ---------------------- | --------------------- |
| `fallback-lru` among model-needing misses  | 33.7% (1,111)          | 0.1% (4)              |
| fallbacks after the first 100 requests     | 34.2%                  | 0.0%                  |
| model-decided miss latency p50 / p99 / max | 6.12 / 9.38 / 10.12 ms | 3.36 / 4.46 / 6.90 ms |
| mean server-side latency per request       | 4.14 ms                | 1.62 ms               |

The four remaining fallbacks are requests 0 to 3, while the lazy gRPC
connection was being established. For comparison, a separate earlier run
under lru on a different request sequence had a mean server-side latency
of 0.68 ms.

### Trade-offs / Limitations

- The cause is open. CPU power management was suspected (the same call is
  slower after idle gaps, in-process and over gRPC) but a power-profile
  experiment was prepared and not run.
- Spaced gRPC calls are still about 2.7 ms slower than back-to-back calls
  (3.20 against 0.48 ms p50). The slowest model-decided miss was 6.9 ms
  against the 8 ms timeout, so a busier machine or concurrent load can
  bring fallbacks back.
- Policy `ml` adds the decision time to every miss (mean 1.62 ms per
  request here against 0.68 ms under lru in the earlier run), which has to
  be weighed against any hit rate gain.
- Hit rate was 45.07% in the first test (mixed deciders) and 47.03% in the
  second, against 42.93% for an offline LRU(250) simulation on the same
  sequence. This is not evidence about the model: the workload used
  seed 42, the seed of the run the model was trained on (Experiment 1), and
  lru was simulated, not run.
- Both smoke tests ran before the changes were committed, so they are not
  logged in `experiment-log.md`.
