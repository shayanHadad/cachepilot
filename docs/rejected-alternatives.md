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
