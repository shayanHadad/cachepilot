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