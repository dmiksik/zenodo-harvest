#!/usr/bin/env python3
"""
harvest_day.py — Harvest a single heavy day from Zenodo with recursive time splitting.

Použití:
    python harvest_day.py --day 2023-12-28 --out-dir ./zenodo_day \
      --main-jsonl zenodo_dump/raw/records.jsonl

Parametry:
  --day YYYY-MM-DD      Konkrétní den (UTC)
  --out-dir PATH        Kam uložit výsledky
  --main-jsonl PATH     Hlavní records.jsonl, proti kterému se deduplikuje (volitelné)
  --size INT            Velikost stránky (<=1000, default 100)
  --throttle-per-minute INT   Počet požadavků za minutu (default 55)
  --hard-cap INT        Limit záznamů na okno před dělením (default 9000)

Výstupy:
  <DAY>_raw.jsonl   – všechna data ze zadaného dne
  <DAY>_dedup.jsonl – odduplikovaná verze (podle id + proti hlavnímu JSONL)
"""

import argparse, json, math, os, sys, time
from datetime import datetime, timezone, timedelta
import requests

Z_API = "https://zenodo.org/api/records"

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def get_total(sess, q: str) -> int:
    r = sess.get(Z_API, params={"q": q, "all_versions":"false", "size":0}, timeout=60)
    r.raise_for_status()
    hits = r.json().get("hits", {})
    total = hits.get("total", 0)
    if isinstance(total, dict):
        total = total.get("value", 0)
    return int(total)

def fetch_window(sess, q: str, size: int, throttle_s: float, out_fh):
    page = 1
    while True:
        params = {"q": q, "all_versions":"false", "size": size, "page": page, "sort":"mostrecent"}
        r = sess.get(Z_API, params=params, timeout=120)
        r.raise_for_status()
        hits = r.json().get("hits", {}).get("hits", [])
        if not hits:
            break
        for rec in hits:
            out_fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        page += 1
        time.sleep(throttle_s)

def split_until_cap(sess, start, end, hard_cap, throttle_s):
    stack = [(start, end)]
    result = []
    while stack:
        s, e = stack.pop()
        q = f"(resource_type.type:dataset) AND created:[{iso_z(s)} TO {iso_z(e)}]"
        total = get_total(sess, q)
        if total <= hard_cap or (e-s).total_seconds() <= 1:
            result.append((s, e)); time.sleep(throttle_s); continue
        mid = s + (e-s)/2
        mid_floor = s + timedelta(seconds=int((e-s).total_seconds()/2))
        if mid_floor <= s: mid_floor = s + timedelta(seconds=1)
        if mid_floor >= e: mid_floor = e - timedelta(seconds=1)
        stack.append((mid_floor, e))
        stack.append((s, mid_floor))
        time.sleep(throttle_s)
    return result

def dedupe_day_output(day_raw, main_jsonl, day_dedup):
    seen = set()
    if main_jsonl and os.path.exists(main_jsonl):
        with open(main_jsonl, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    rid = json.loads(line).get("id")
                    if rid: seen.add(rid)
                except: pass
    in_count, out_count = 0,0
    with open(day_raw,"r",encoding="utf-8") as inp, open(day_dedup,"w",encoding="utf-8") as outp:
        for line in inp:
            in_count += 1
            try:
                rec = json.loads(line)
            except: continue
            rid = rec.get("id")
            if not rid: continue
            if rid in seen: continue
            seen.add(rid)
            outp.write(line)
            out_count += 1
    return in_count, out_count

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--main-jsonl", default=None)
    ap.add_argument("--size", type=int, default=100)
    ap.add_argument("--throttle-per-minute", type=int, default=55)
    ap.add_argument("--hard-cap", type=int, default=9000)
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    day_dt = datetime.strptime(args.day,"%Y-%m-%d").replace(tzinfo=timezone.utc)
    start = day_dt.replace(hour=0,minute=0,second=0)
    end   = day_dt.replace(hour=23,minute=59,second=59)

    raw_path   = os.path.join(args.out_dir, f"{args.day}_raw.jsonl")
    dedup_path = os.path.join(args.out_dir, f"{args.day}_dedup.jsonl")

    throttle_s = 60.0/max(1,args.throttle_per_minute)

    with requests.Session() as sess:
        windows = split_until_cap(sess,start,end,args.hard_cap,throttle_s)
        with open(raw_path,"w",encoding="utf-8") as out_fh:
            for s,e in sorted(windows):
                q = f"(resource_type.type:dataset) AND created:[{iso_z(s)} TO {iso_z(e)}]"
                fetch_window(sess,q,args.size,throttle_s,out_fh)

    in_c,out_c = dedupe_day_output(raw_path,args.main_jsonl,dedup_path)
    print(f"Day {args.day}: RAW {in_c} → Dedup {out_c}")
    print(f"RAW:   {raw_path}")
    print(f"DEDUP: {dedup_path}")

if __name__=="__main__":
    main()

