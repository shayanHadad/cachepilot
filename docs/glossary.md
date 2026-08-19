# CachePilot — Technical Decisions Glossary (Quick Reference)

A quick-scan index of technical decisions made throughout the
project. For full details on any decision, see `architecture.md` or
the `DEV NOTE` comments inside the relevant source file.

| Decision                                                                               | One-line reason                                                                         |
| -------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `Timestamp` as `int64` (Unix ms), not `time.Time`                                      | JSON/pandas compatibility between Go and Python                                         |
| `CacheStatus` as a distinct type (`type CacheStatus string`)                           | discoverability/documentation, not full typo prevention                                 |
| `Source` (in `CacheDecision`) is a raw string, not a distinct type                     | only one call site constructs it (`manager.go`), low risk of inconsistency              |
| `Hits/Misses/Evictions` counters use `atomic.Int64`, not `Mutex`                       | independent counters, no need for multi-field coordination                              |
| `internal/store` (not `internal/mongo`)                                                | name collision with the official `mongo-driver` package                                 |
| Cache stores `value []byte` (not a typed struct)                                       | full decoupling of the cache from the domain schema                                     |
| `config.yaml` + env var overrides                                                      | reproducibility for experiment runs + deployment flexibility                            |
| Logger: drop-on-full instead of blocking                                               | keeps the critical path lightweight; a deliberate trade-off accepting partial data loss |
| `log/slog` instead of manual `encoding/json`                                           | standard library package, no new dependency, built-in log levels                        |
| Burst as a toggleable flag in the workload generator                                   | enables a direct comparison between stationary and bursty access patterns               |
| `query_type` derived from the document itself (`media_size_kb`), not a synthetic label | a natural, meaningful feature for ML, without manual injection                          |
| ML model: offline training, inference-only at runtime                                  | keeps the critical path fast, reproducibility, explainability                           |
| 5–10ms fallback timeout for gRPC                                                       | prevents the main path from slowing down when the ML service is slow/unavailable        |

<!-- Add new rows here whenever an important technical decision is made -->
