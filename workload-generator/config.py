"""
Configuration for the workload generator.

"""

from dataclasses import dataclass


@dataclass
class WorkloadConfig:
    # Where to find the posts to build requests from, and where to
    # send those requests.
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "cachepilot"
    service_url: str = "http://localhost:8080"

    # Zipfian access pattern. Higher zipf_param means a smaller
    # number of posts absorb a much larger share of requests (a more
    # "unfair"/skewed distribution). 1.0-1.5 is a typical range for
    # modeling real-world popularity skew.
    zipf_param: float = 1.2

    # How much traffic to generate.
    total_requests: int = 10000
    requests_per_sec: float = 50.0

    # Fixed seed so a run can be reproduced exactly later — record
    # this value in docs/experiment-log.md alongside the run's
    # results.
    seed: int = 42

    # Burst simulation — off by default (see docs/architecture.md's
    # discussion of why this is a toggle, not always-on).
    enable_burst: bool = False
    # Probability, per second, that a new burst starts.
    burst_rate: float = 0.02
    # How many times more likely a bursting key is to be picked,
    # relative to its normal Zipfian weight.
    burst_intensity: float = 5.0
    burst_duration_sec: float = 30.0

    # Where per-request results and burst ground truth get written.
    results_path: str = "../data/raw_logs/workload_results.jsonl"
    burst_ground_truth_path: str = "../data/raw_logs/burst_ground_truth.jsonl"