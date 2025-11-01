#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, json, os, csv, hashlib, sys
from collections import Counter, defaultdict
from datetime import datetime

def norm_hash(obj):
    """Stabilní hash JSON obsahu (seřazené klíče)."""
    s = json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(s.encode('utf-8')).hexdigest(), len(s)

def pick(d, *path, default=None):
    v = d
    for k in path:
        if isinstance(v, dict) and k in v:
            v = v[k]
        else:
            return default
    return v

def created_in_range(created, start_excl=None, end_excl=None):
    if not isinstance(created, str):
        return False
    # Porovnáváme stringově, aby prošly i „neplné“ tvary (YYYY-MM, YYYY).
    # Vytvoříme si hrubé hranice: pokud jsou krátké, zkrátíme i created.
    s_ok = True
    e_ok = True
    if start_excl:
        s_ok = created >= start_excl
    if end_excl:
        e_ok = created < end_excl
    return s_ok and e_ok

def main():
    ap = argparse.ArgumentParser(description="Extract duplicate IDs from Zenodo NDJSON and render side-by-side views.")
    ap.add_argument("--input", default="records.jsonl", help="Input NDJSON (one JSON per line).")
    ap.add_argument("--out-dir", default="dedupe_report", help="Output directory.")
    ap.add_argument("--filter-dataset-published", action="store_true",
                    help="Keep only records with metadata.resource_type.type=='dataset' AND status=='published'.")
    ap.add_argument("--start-created", default=None,
                    help="Lower bound (inclusive) for created, e.g. 1990-01-01 or 1990-01-01T00:00:00Z. String compare.")
    ap.add_argument("--end-created", default=None,
                    help="Upper bound (exclusive) for created, e.g. 2025-11-02 or 2025-11-02T00:00:00Z. String compare.")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    inp = args.input

    # --- PASS 1: spočítat výskyty id (jen s případnými filtry) ---
    counts = Counter()
    with open(inp, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            if args.filter_dataset_published:
                if pick(obj, "metadata", "resource_type", "type") != "dataset":
                    continue
                if obj.get("status") != "published":
                    continue

            if args.start_created or args.end_created:
                if not created_in_range(obj.get("created"), args.start_created, args.end_created):
                    continue

            i = obj.get("id")
            if i is not None:
                counts[str(i)] += 1

    dup_ids = [i for i, c in counts.items() if c > 1]
    print(f"Total IDs seen: {len(counts)}; duplicates: {len(dup_ids)}", file=sys.stderr)
    if not dup_ids:
        print("No duplicates found with given filters.")
        return

    # --- PASS 2: sesbírat duplicitní záznamy ---
    groups = defaultdict(list)
    with open(inp, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            if args.filter_dataset_published:
                if pick(obj, "metadata", "resource_type", "type") != "dataset":
                    continue
                if obj.get("status") != "published":
                    continue

            if args.start_created or args.end_created:
                if not created_in_range(obj.get("created"), args.start_created, args.end_created):
                    continue

            i = obj.get("id")
            if i is None:
                continue
            i = str(i)
            if counts[i] > 1:
                groups[i].append((obj, line))

    # --- Výstupy ---
    # 1) summary CSV
    sum_path = os.path.join(args.out_dir, "duplicates_summary.csv")
    with open(sum_path, "w", encoding="utf-8", newline="") as wf:
        w = csv.writer(wf)
        w.writerow(["id", "count", "unique_hashes", "all_identical",
                    "created_list", "updated_list", "revisions", "titles"])
        for i, items in sorted(groups.items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else kv[0]):
            hashes = set()
            createds = []
            updateds = []
            revisions = []
            titles = []
            for obj, _ in items:
                h, _ln = norm_hash(obj); hashes.add(h)
                createds.append(obj.get("created", ""))
                updateds.append(obj.get("updated", ""))
                revisions.append(str(obj.get("revision", "")))
                titles.append((pick(obj, "metadata", "title") or "")[:120])
            w.writerow([i, len(items), len(hashes), (len(hashes) == 1),
                        "|".join(createds), "|".join(updateds), "|".join(revisions), " | ".join(titles)])
    print(f"Wrote {sum_path}")

    # 2) pairs CSV (A vs B – „vedle sebe“)
    #    Pokud má skupina >2 záznamy, vytváříme řádky: (1 vs 2), (1 vs 3), (1 vs 4), ...
    pairs_path = os.path.join(args.out_dir, "duplicates_pairs.csv")
    with open(pairs_path, "w", encoding="utf-8", newline="") as wf:
        w = csv.writer(wf)
        w.writerow([
            "id",
            "A.created","A.updated","A.revision","A.status","A.doi","A.conceptdoi","A.title","A.hash","A.json_length",
            "B.created","B.updated","B.revision","B.status","B.doi","B.conceptdoi","B.title","B.hash","B.json_length",
            "hash_equal"
        ])
        for i, items in groups.items():
            # připrav si (obj, hash, len, metadatové výřezy)
            prepared = []
            for obj, line in items:
                h, ln = norm_hash(obj)
                prepared.append({
                    "created": obj.get("created",""),
                    "updated": obj.get("updated",""),
                    "revision": obj.get("revision",""),
                    "status": obj.get("status",""),
                    "doi": obj.get("doi",""),
                    "conceptdoi": obj.get("conceptdoi",""),
                    "title": (pick(obj, "metadata", "title") or "")[:160],
                    "hash": h,
                    "json_length": ln,
                })
            base = prepared[0]
            for other in prepared[1:]:
                w.writerow([
                    i,
                    base["created"], base["updated"], base["revision"], base["status"], base["doi"], base["conceptdoi"], base["title"], base["hash"], base["json_length"],
                    other["created"], other["updated"], other["revision"], other["status"], other["doi"], other["conceptdoi"], other["title"], other["hash"], other["json_length"],
                    "yes" if base["hash"] == other["hash"] else "no"
                ])
    print(f"Wrote {pairs_path}")

    # 3) groups JSONL (každé id na jeden řádek; pro detailní ruční diff)
    groups_path = os.path.join(args.out_dir, "duplicates_groups.jsonl")
    with open(groups_path, "w", encoding="utf-8") as wf:
        for i, items in groups.items():
            docs = []
            for obj, line in items:
                h, ln = norm_hash(obj)
                docs.append({
                    "created": obj.get("created",""),
                    "updated": obj.get("updated",""),
                    "revision": obj.get("revision",""),
                    "status": obj.get("status",""),
                    "doi": obj.get("doi",""),
                    "conceptdoi": obj.get("conceptdoi",""),
                    "title": pick(obj, "metadata", "title"),
                    "hash": h,
                    "json_length": ln,
                    "raw": obj,   # celý objekt pro ruční inspekci
                })
            out = {"id": i, "count": len(items), "docs": docs}
            wf.write(json.dumps(out, ensure_ascii=False) + "\n")
    print(f"Wrote {groups_path}")

    # 4) volitelný rychlý přehled distribuce počtů
    dist = Counter(len(v) for v in groups.values())
    print("Duplicate count distribution (occurrences -> how many IDs):", dict(sorted(dist.items())), file=sys.stderr)

if __name__ == "__main__":
    main()

