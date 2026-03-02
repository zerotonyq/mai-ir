import argparse
import json
import os
from pathlib import Path

from pymongo import MongoClient, UpdateOne


def read_env(path: str) -> dict:
    env = {}
    if not os.path.exists(path):
        return env
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def build_default_mongo_url() -> str:
    env = read_env(".env")
    user = env.get("MONGO_ROOT_USER", "admin")
    pwd = env.get("MONGO_ROOT_PASSWORD", "password")
    return f"mongodb://{user}:{pwd}@localhost:27017/?authSource=admin"


def iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def collect_records(input_path: Path) -> list[dict]:
    records = []
    if input_path.is_dir():
        for p in sorted(input_path.glob("*.jsonl")):
            records.extend(iter_jsonl(p))
    else:
        records.extend(iter_jsonl(input_path))
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description="Load scraped articles into MongoDB.")
    parser.add_argument("--input", default="task_1/data/parsed/all.jsonl")
    parser.add_argument("--mongo-url", default=os.getenv("MONGO_URL") or build_default_mongo_url())
    parser.add_argument("--db", default="infosearch")
    parser.add_argument("--collection", default="articles")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise SystemExit(f"Input path does not exist: {input_path}")

    records = collect_records(input_path)
    if not records:
        print("No records to load.")
        return 0

    client = MongoClient(args.mongo_url)
    collection = client[args.db][args.collection]
    collection.create_index("url", unique=True)

    ops = []
    for r in records:
        url = r.get("url")
        if not url:
            url = r.get("text")
        if not url:
            continue
        ops.append(UpdateOne({"url": url}, {"$set": r}, upsert=True))

    if ops:
        res = collection.bulk_write(ops, ordered=False)
        print(
            f"Upserts: {res.upserted_count}, modified: {res.modified_count}, matched: {res.matched_count}"
        )
    else:
        print("No valid records with url.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
