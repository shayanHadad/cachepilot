"""
gRPC server for DecisionService (see proto/cache_decision.proto).

Only job here is translating between the wire format and
model.inference.decide() — no decision logic lives in this file, so
swapping the decider is a change to inference.py alone.

Usage:
    pip install -r requirements.txt
    python server/grpc_server.py
"""

import logging
import sys
from concurrent import futures
from pathlib import Path

import grpc
import yaml

# decisionpb is generated code, not a normal source file — see its
# __init__.py for the regeneration command.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from decisionpb import cache_decision_pb2 as pb
from decisionpb import cache_decision_pb2_grpc as pb_grpc
from model import inference

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("grpc_server")

DEFAULT_CONFIG = {
    "grpc_addr": "0.0.0.0:50051",
    "decision_mode": "model",
    "model_path": "model/artifacts/model.txt",
}


class DecisionServicer(pb_grpc.DecisionServiceServicer):
    def Decide(self, request: pb.DecisionRequest, context) -> pb.DecisionResponse:
        try:
            result = inference.decide(
                key=request.key,
                frequency_1min=request.frequency_1min,
                frequency_5min=request.frequency_5min,
                recency_sec=request.recency_sec,
                inter_arrival_avg=request.inter_arrival_avg,
                payload_size_kb=request.payload_size_kb,
                query_type=request.query_type,
            )
        except inference.InvalidRequestError as e:
            # The Go client treats any error as "use fallback-lru", so a
            # request we can't score is never answered with a guess.
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        return pb.DecisionResponse(
            admit=result.admit,
            ttl_ms=result.ttl_ms,
            source=result.source,
        )


def load_config(path: str | Path | None = None) -> dict:
    # Default path is relative to this file's location (ml-service/),
    # not the current working directory — otherwise running this
    # script from a different folder would silently look for
    # config.yaml in the wrong place. Same issue we hit and fixed for
    # go-cache-service's config.
    if path is None:
        path = Path(__file__).resolve().parent.parent / "config.yaml"
    path = Path(path).resolve()

    with open(path, encoding="utf-8") as f:
        config = {**DEFAULT_CONFIG, **(yaml.safe_load(f) or {})}

    # model_path is a filesystem path, so it resolves against the
    # config file's directory, like logging.path does on the Go side.
    model_path = Path(config["model_path"])
    if not model_path.is_absolute():
        model_path = path.parent / model_path
    config["model_path"] = str(model_path)
    return config


def serve(addr: str) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb_grpc.add_DecisionServiceServicer_to_server(DecisionServicer(), server)
    server.add_insecure_port(addr)
    server.start()
    log.info(f"listening on {addr}")
    server.wait_for_termination()


if __name__ == "__main__":
    config = load_config()
    try:
        inference.init(config["decision_mode"], config["model_path"])
    except inference.InferenceInitError as e:
        log.error(f"cannot start: {e}")
        sys.exit(1)
    log.info(f"decider ready: {inference.describe()}")
    serve(config["grpc_addr"])