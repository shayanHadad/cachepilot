"""
gRPC server for DecisionService (see proto/cache_decision.proto).

Only job here is translating between the wire format and
model.inference.decide() — no decision logic lives in this file, so
swapping the heuristic for a real model later is a one-file change.

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
from model.inference import decide

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("grpc_server")


class DecisionServicer(pb_grpc.DecisionServiceServicer):
    def Decide(self, request: pb.DecisionRequest, context) -> pb.DecisionResponse:
        result = decide(
            key=request.key,
            frequency_1min=request.frequency_1min,
            frequency_5min=request.frequency_5min,
            recency_sec=request.recency_sec,
            inter_arrival_avg=request.inter_arrival_avg,
            payload_size_kb=request.payload_size_kb,
            query_type=request.query_type,
        )
        return pb.DecisionResponse(
            admit=result.admit,
            ttl_ms=result.ttl_ms,
            source=result.source,
        )


def load_config(path: str | None = None) -> dict:
    # Default path is relative to this file's location (ml-service/),
    # not the current working directory — otherwise running this
    # script from a different folder would silently look for
    # config.yaml in the wrong place. Same issue we hit and fixed for
    # go-cache-service's config.
    if path is None:
        path = Path(__file__).resolve().parent.parent / "config.yaml"
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def serve(addr: str) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb_grpc.add_DecisionServiceServicer_to_server(DecisionServicer(), server)
    server.add_insecure_port(addr)
    server.start()
    log.info(f"listening on {addr}")
    server.wait_for_termination()


if __name__ == "__main__":
    config = load_config()
    serve(config["grpc_addr"])