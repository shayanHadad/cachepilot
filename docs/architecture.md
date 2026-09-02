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

---

## Narrowed Scope of `ml_decision.go`; gRPC Client Design

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
