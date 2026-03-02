import json

src = [
    "task_1/data/parsed/kommersant.jsonl",
    "task_1/data/parsed/rbc.jsonl",
]
out = "task_1/data/parsed/all.jsonl"

seen = set()
with open(out, "w", encoding="utf-8") as w:
    for path in src:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                url = obj.get("text")
                if not url or url in seen:
                    continue
                seen.add(url)
                w.write(json.dumps(obj, ensure_ascii=False) + "\n")

print("written:", out, "records:", len(seen))