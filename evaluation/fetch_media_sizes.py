"""
Saves the post id order and media_size_kb from MongoDB for offline_compare.py.

The ml-service venv has no pymongo, so this runs separately (with
/usr/bin/python3) and offline_compare.py reads the JSON it writes. The
ordered id list is fingerprinted with the generator's own hash function:
if the fingerprint differs from the run_config.json of a real run, the
order differs from the generator's and rank-to-post mapping is wrong.

Usage:
    /usr/bin/python3 evaluation/fetch_media_sizes.py [--order natural|id] [--expect-sha256 PREFIX]
"""

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "workload-generator"))

from workload_plan import hash_post_ids  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    p.add_argument("--mongo-db", default="cachepilot")
    p.add_argument("--order", choices=("natural", "id"), default="natural",
                   help="natural = collection order, id = sorted by _id")
    p.add_argument("--expect-sha256", default=None, help="fail unless post_ids_sha256 starts with this")
    p.add_argument("--out", type=Path, default=ROOT / "data" / "eval" / "media_sizes.json")
    args = p.parse_args()

    from pymongo import MongoClient

    cursor = MongoClient(args.mongo_uri)[args.mongo_db]["posts"].find({}, {"media_size_kb": 1})
    if args.order == "id":
        cursor = cursor.sort("_id", 1)
    docs = list(cursor)
    ids = [str(d["_id"]) for d in docs]
    sizes = [float(d.get("media_size_kb", 0.0)) for d in docs]
    digest = hash_post_ids(ids)

    print(f"posts {len(ids)}  with media {sum(1 for s in sizes if s > 0)}  order {args.order}")
    print(f"post_ids_sha256 {digest}")
    if args.expect_sha256 and not digest.startswith(args.expect_sha256):
        raise SystemExit(f"fingerprint does not start with {args.expect_sha256}; try --order id, or the order differs from the generator's")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps({"post_ids_sha256": digest, "ids": ids, "media_size_kb": sizes}), encoding="utf-8")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
