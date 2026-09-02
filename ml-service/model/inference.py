"""
Turns request features into an admit/TTL decision.

Right now this is a simple rule, not a trained model — good enough to
get the Go <-> Python gRPC path working end to end before the real
training data exists. grpc_server.py only calls decide(), so swapping
this rule out for a loaded LightGBM model later shouldn't require
touching the server code at all.
"""

from dataclasses import dataclass


@dataclass
class Decision:
    admit: bool
    ttl_ms: int
    source: str


# Anything hit at least once in the last minute gets cached, since
# that's already a real signal of recurring interest. Everything
# else is treated as a one-off and skipped, rather than filling the
# cache with things unlikely to be asked for again.
MIN_FREQUENCY_1MIN_TO_ADMIT = 1

# Flat TTL for now — 5 minutes felt like a reasonable starting point,
# not derived from anything. Worth tuning once real data is around
# to look at.
DEFAULT_TTL_MS = 5 * 60 * 1000


def decide(
    key: str,
    frequency_1min: int,
    frequency_5min: int,
    recency_sec: float,
    inter_arrival_avg: float,
    payload_size_kb: float,
    query_type: str,
) -> Decision:
    admit = frequency_1min >= MIN_FREQUENCY_1MIN_TO_ADMIT
    return Decision(
        admit=admit,
        ttl_ms=DEFAULT_TTL_MS if admit else 0,
        source="heuristic-v1",
    )