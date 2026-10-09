"""
Turns request features into an admit/TTL decision.

Two modes, chosen by decision_mode in config.yaml:

  model      scores each request with the trained LightGBM model.
  heuristic  a plain frequency rule, kept as an explicit baseline.

init() must be called once at startup. It never falls back silently:
if the model is missing or doesn't match its metadata, it raises
InferenceInitError, so the service fails to start instead of quietly
answering with a different decider. Go already has its own fallback
(fallback-lru) for an unreachable or failing ML service.

grpc_server.py only calls init() and decide(), so this file is the
only place that knows how a decision is made.
"""

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import lightgbm as lgb
import pandas as pd

from features.feature_engineering import CATEGORICAL_COLUMNS, FEATURE_COLUMNS, prepare_features


@dataclass
class Decision:
    admit: bool
    ttl_ms: int
    source: str


class InferenceInitError(Exception):
    """Raised at startup when the configured decider can't be set up."""


class InvalidRequestError(ValueError):
    """Raised when a request can't be scored, e.g. an unknown query_type."""


HEURISTIC_SOURCE = "heuristic-v1"

# Heuristic mode: anything hit at least once in the last minute is
# treated as recurring interest and cached; everything else is skipped.
MIN_FREQUENCY_1MIN_TO_ADMIT = 1

# Flat TTL in both modes. The model only predicts admit/reject, so TTL
# is not model-driven yet. 5 minutes was a starting point, not derived
# from anything.
DEFAULT_TTL_MS = 5 * 60 * 1000

REQUIRED_META_KEYS = (
    "model_name",
    "model_sha256",
    "best_iteration",
    "reject_score_threshold",
    "feature_names",
)


@dataclass(frozen=True)
class _LoadedModel:
    booster: lgb.Booster
    name: str
    threshold: float
    best_iteration: int
    # query_type values seen in training, or None if the model file doesn't expose them.
    query_types: frozenset | None


_mode: str | None = None
_loaded: _LoadedModel | None = None


def init(mode: str, model_path: str | Path | None = None) -> None:
    """Sets up the decider for the given mode. Raises InferenceInitError
    if it can't be used."""
    global _mode, _loaded

    if mode == "heuristic":
        _loaded = None
    elif mode == "model":
        if model_path is None:
            raise InferenceInitError("decision_mode is 'model' but no model_path was given")
        _loaded = _load_model(Path(model_path))
    else:
        raise InferenceInitError(f"unknown decision_mode {mode!r} (expected 'model' or 'heuristic')")
    _mode = mode


def describe() -> str:
    """One-line summary of the active decider, for the startup log."""
    if _mode == "heuristic":
        return f"mode=heuristic source={HEURISTIC_SOURCE}"
    if _mode == "model" and _loaded is not None:
        return (f"mode=model source={_loaded.name} "
                f"reject_threshold={_loaded.threshold:.4f} best_iteration={_loaded.best_iteration}")
    return "not initialized"


def _load_model(model_path: Path) -> _LoadedModel:
    meta_path = model_path.with_name(model_path.stem + "_meta.json")

    if not model_path.is_file():
        raise InferenceInitError(
            f"model file not found: {model_path} (train one with ml-service/model/train.py)"
        )
    if not meta_path.is_file():
        raise InferenceInitError(f"model metadata not found: {meta_path}")

    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise InferenceInitError(f"{meta_path} is not valid JSON: {e}") from e

    missing = [k for k in REQUIRED_META_KEYS if k not in meta]
    if missing:
        raise InferenceInitError(
            f"{meta_path} is missing {missing}; re-run train.py so the model and its metadata are written together"
        )

    digest = hashlib.sha256(model_path.read_bytes()).hexdigest()
    if digest != meta["model_sha256"]:
        raise InferenceInitError(
            f"{model_path} does not match the sha256 recorded in {meta_path}; "
            f"the two files come from different training runs"
        )

    try:
        booster = lgb.Booster(model_file=str(model_path))
    except lgb.basic.LightGBMError as e:
        raise InferenceInitError(f"could not load {model_path}: {e}") from e

    for source, names in (("model file", booster.feature_name()), ("metadata", meta["feature_names"])):
        if list(names) != FEATURE_COLUMNS:
            raise InferenceInitError(
                f"feature mismatch: the {source} lists {list(names)} but the serving code builds "
                f"{FEATURE_COLUMNS}; retrain or fix feature_engineering.py"
            )

    query_types = None
    categories = getattr(booster, "pandas_categorical", None)
    if categories:
        query_types = frozenset(categories[CATEGORICAL_COLUMNS.index("query_type")])

    return _LoadedModel(
        booster=booster,
        name=str(meta["model_name"]),
        threshold=float(meta["reject_score_threshold"]),
        best_iteration=int(meta["best_iteration"]),
        query_types=query_types,
    )


def admit_probability(
    frequency_1min: int,
    frequency_5min: int,
    recency_sec: float,
    inter_arrival_avg: float,
    payload_size_kb: float,
    query_type: str,
) -> float:
    """Returns the model's probability that this request should be admitted."""
    if _mode != "model" or _loaded is None:
        raise RuntimeError("admit_probability() needs init('model', ...) first")

    if _loaded.query_types is not None and query_type not in _loaded.query_types:
        raise InvalidRequestError(
            f"unknown query_type {query_type!r}; the model was trained on {sorted(_loaded.query_types)}"
        )

    row = pd.DataFrame([{
        "frequency_1min": frequency_1min,
        "frequency_5min": frequency_5min,
        "recency_sec": recency_sec,
        "inter_arrival_avg": inter_arrival_avg,
        "payload_size_kb": payload_size_kb,
        "query_type": query_type,
    }])
    # num_threads=1: a single row isn't worth spinning up OpenMP threads for.
    p = _loaded.booster.predict(
        prepare_features(row), num_iteration=_loaded.best_iteration, num_threads=1
    )
    return float(p[0])


def decide(
    key: str,
    frequency_1min: int,
    frequency_5min: int,
    recency_sec: float,
    inter_arrival_avg: float,
    payload_size_kb: float,
    query_type: str,
) -> Decision:
    if _mode == "heuristic":
        admit = frequency_1min >= MIN_FREQUENCY_1MIN_TO_ADMIT
        return Decision(admit=admit, ttl_ms=DEFAULT_TTL_MS if admit else 0, source=HEURISTIC_SOURCE)

    if _mode == "model" and _loaded is not None:
        p_admit = admit_probability(
            frequency_1min, frequency_5min, recency_sec,
            inter_arrival_avg, payload_size_kb, query_type,
        )
        # Same rule as train.py: reject when the reject score reaches the tuned threshold.
        is_reject = (1.0 - p_admit) >= _loaded.threshold
        admit = not is_reject
        return Decision(admit=admit, ttl_ms=DEFAULT_TTL_MS if admit else 0, source=_loaded.name)

    raise RuntimeError("inference.init() has not been called")