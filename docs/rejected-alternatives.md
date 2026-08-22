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

**Date:** (date of the original architecture decision)

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

- **TLS on the Go ↔ ML service gRPC connection** — rejected in favor
  of a plain, unencrypted connection. Reasons: the connection is only
  ever made over the internal docker-compose network, never a public
  one, so TLS's threat model (network eavesdropping, server
  impersonation) doesn't apply; the added handshake cost also isn't
  worth paying on a path with a strict 5-10ms budget. Revisit
  condition: if the Go and ML services are ever deployed on separate,
  untrusted networks instead of a single docker-compose stack — at
  that point this decision should be re-evaluated from scratch, not
  just patched.

---

## Writing the Workload Generator in Go Instead of Python

**Date:** during workload-generator implementation

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
