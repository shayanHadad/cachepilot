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
