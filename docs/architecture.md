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
