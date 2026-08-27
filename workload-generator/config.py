"""
Config for the workload generator.

Plain dataclass instead of YAML, unlike the Go service — this script
gets re-run with different flags for every experiment, so CLI
overrides make more sense than editing a shared file each time.
"""

from dataclasses import dataclass


@dataclass
class WorkloadConfig:
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "cachepilot"
    service_url: str = "http://localhost:8080"

    # Higher = more skewed (a few posts hog most of the traffic).
    # 1.0-1.5 is a typical range for modeling popularity skew.
    zipf_param: float = 1.2

    total_requests: int = 10000
    requests_per_sec: float = 50.0

    # Fixed seed for reproducible runs — log this in experiment-log.md.
    seed: int = 42

    # Off by default (see docs/architecture.md for why it's a toggle).
    enable_burst: bool = False
    burst_rate: float = 0.02       # chance per second a new burst starts
    burst_intensity: float = 5.0   # weight multiplier while bursting
    burst_duration_sec: float = 30.0
    # A post has to sit idle this long before it's eligible to burst —
    # otherwise "burst" could just re-hit an already-hot post, which
    # wouldn't tell us anything (see zipfian_generator.py).
    burst_min_idle_sec: float = 10.0

    # Each run writes to its own timestamped folder under this root
    # (see create_run_dir), so runs never clobber each other.
    output_root: str = "../data/raw_logs"

    # The Go service's own log — copied into the run folder at the
    # end so everything from one run lives in one place.
    go_service_log_path: str = "../data/raw_logs/service.jsonl"