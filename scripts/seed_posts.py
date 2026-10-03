"""
Seeds the `posts` collection with sample documents, for manual local
testing of the go-cache-service before the real workload-generator
exists.

This is a throwaway development utility, not part of the project's
formal pipeline (data-pipeline/, workload-generator/) — it exists
purely to let you run the Go service against real data and confirm
the whole chain (HTTP -> cache -> Mongo) actually works end to end.

Usage:
    pip install pymongo --break-system-packages
    python scripts/seed_posts.py --count 200

Then note a few of the printed _id values to test with:
    curl "http://localhost:8080/get?key=<one of the printed ids>"
"""

import argparse
import random
import string
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient


def random_content(min_words=5, max_words=80):
    words = ["lorem", "ipsum", "post", "update", "today", "thoughts",
             "cachepilot", "mongodb", "golang", "weekend", "coffee",
             "project", "launch", "release", "photo", "video", "trip"]
    n = random.randint(min_words, max_words)
    return " ".join(random.choices(words, k=n))


def random_tags():
    pool = ["tech", "life", "travel", "food", "music", "sports", "news"]
    return random.sample(pool, k=random.randint(0, 3))


def make_post(author_pool, created_base):
    is_media = random.random() < 0.35  # ~35% of posts carry media
    return {
        "author_id": random.choice(author_pool),
        "content": random_content(),
        "created_at": created_base - timedelta(minutes=random.randint(0, 60 * 24 * 14)),
        "likes_count": random.randint(0, 5000),
        "comments_count": random.randint(0, 500),
        "media_size_kb": round(random.uniform(50, 4000), 1) if is_media else 0.0,
        "tags": random_tags(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uri", default="mongodb://localhost:27017", help="MongoDB URI")
    parser.add_argument("--db", default="cachepilot", help="Database name")
    parser.add_argument("--count", type=int, default=200, help="Number of posts to insert")
    parser.add_argument("--authors", type=int, default=20, help="Number of distinct authors to spread posts across")
    parser.add_argument("--wipe", action="store_true", help="Delete existing posts before seeding")
    args = parser.parse_args()

    client = MongoClient(args.uri)
    db = client[args.db]
    coll = db["posts"]

    if args.wipe:
        deleted = coll.delete_many({}).deleted_count
        print(f"Wiped {deleted} existing documents from posts.")

    author_pool = [
        "user_" + "".join(random.choices(string.ascii_lowercase, k=6))
        for _ in range(args.authors)
    ]
    created_base = datetime.now(timezone.utc)

    docs = [make_post(author_pool, created_base) for _ in range(args.count)]
    result = coll.insert_many(docs)

    print(f"Inserted {len(result.inserted_ids)} posts into {args.db}.posts")
    print("\nSample ids to test with:")
    for oid in result.inserted_ids[:10]:
        print(f"  {oid}")
    print(f"\nExample:\n  curl \"http://localhost:8080/get?key={result.inserted_ids[0]}\"")


if __name__ == "__main__":
    main()