#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
harvest_day.py — Harvest a single heavy day from Zenodo (datasets, all_versions=false)
by recursively splitting the day into smaller time windows until each window
has <= --hard-cap results. Uses created timestamps, UTC, with right-exclusive windows.

Použití:
    python harvest_day.py --day 2023-12-28 --out-dir ./zenodo_days \
      --main-jsonl zenodo_dump/raw/records.jsonl

Parametry:
  --day YYYY-MM-DD            Den v UTC (povinné)
  --out-dir PATH              Kam uložit výsledky (povinné)
  --main-jsonl PATH           Hlavní records.jsonl pro deduplikaci (volitelné)
  --size INT                  Velikost stránky (<=1000, default 100)
  --throttle-per-minute INT   Požadavků/min (default 55)
  --hard-cap INT              Limit záznamů na okno před dělením (default 9000)

Výstupy:
  <DAY>_raw.jsonl    – všechny stažené záznamy z daného dne (může obsahovat duplicitní id napříč okny)
  <DAY>_dedup.jsonl  – odduplikovaná verze (podle id) a zároveň odfiltrovaná proti --main-jsonl (pokud je zadán)
"""

import argparse
import json
import math
import os
import sys
import time
from datetime import datetime, timezone, timedelta

import requests

Z_API = "https://zenodo.org/api/records"


def iso_z(dt: datetime) -> str:
    """ISO8601 UTC s koncovým 'Z'."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_range(field: str, start_iso: str, end_iso: str,
                inclusive_left: bool = True, inclusive_right: bool = False) -> str:
    """
    Sestaví Lucene range výraz s uvozovkami okolo časů.
    Default tvar: [start TO end}  (levá inkl., pravá EXKLUZIVNÍ)
    """
    left = "[" if inclusive_left else "{"
    right = "]" if inclusive_right else "}"
    return f'{field}:{left}"{start_iso}" TO "{end_iso}"{right}'


def extract_total(data: dict) -> int:
    """Bezpečně vytáhne total z odpovědi Zenodo API (int nebo objekt s .value)."""
    hits = data.get("hits", {})
    total = hits.get("total", 0)
    if isinstance(total, dict):
        total = total.get("value", 0)
    try:
        return int(total)
    except Exception:
        return 0


def get_total(session: requests.Session, q: str, all_versions: bool = False) -> int:
    """Vrátí pouze počet záznamů pro daný dotaz (size=0)."""
    params = {
        "q": q,
        "all_versions": "true" if all_versions else "false",
        "size": 0,
    }
    r = session.get(Z_API, params=params, timeout=60)
    r.raise_for_status()
    return extract_total(r.json())


def fetch_window(session: requests.Session, q: str, size: int, throttle_s: float, out_fh):
    """
    Projede stránky v rámci JEDNOHO pevného okna (q už obsahuje range) a zapisuje JSONL.
    """
    page = 1
    while True:
        params = {
            "q": q,
            "all_versions": "false",
            "size": size,
            "page": page,
            "sort": "mostrecent",  # stabilní pořadí v pevném okně
        }
        r = session.get(Z_API, params=params, timeout=120)
        r.raise_for_status()
        data = r.json()
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break
        for rec in hits:
            out_fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        page += 1
        time.sleep(throttle_s)


def split_until_cap(session: requests.Session,
                    start: datetime, end: datetime,
                    hard_cap: int, throttle_s: float):
    """
    Rekurzivně rozdělí EXKLUZIVNÍ pravé okno [start TO end} na menší okna, dokud
    nebude count <= hard_cap. Vracené položky jsou dvojice (start_dt, end_dt),
    kde end je EXKLUZIVNÍ.
    """
    stack = [(start, end)]
    result = []
    while stack:
        s, e = stack.pop()

        # Odhad počtu v okně [s TO e}
        rng_q = build_range("created", iso_z(s), iso_z(e), inclusive_left=True, inclusive_right=False)
        q = f"(resource_type.type:dataset) AND {rng_q}"
        total = get_total(session, q)

        if total <= hard_cap:
            result.append((s, e))
            time.sleep(throttle_s)
            continue

        # Pokud už okno prakticky nejde řezat (≤1s), přijmi ho
        span = (e - s).total_seconds()
        if span <= 1:
            result.append((s, e))
            time.sleep(throttle_s)
            continue

        # Split v polovině
        mid = s + (e - s) / 2
        # Zajisti posun aspoň o 1 s, aby se to nezacyklilo
        half = int(span // 2)
        mid_floor = s + timedelta(seconds=max(1, half))
        if mid_floor <= s:
            mid_floor = s + timedelta(seconds=1)
        if mid_floor >= e:
            mid_floor = e - timedelta(seconds=1)

        # Dvě EXKLUZIVNÍ pravá okna: [s TO mid} a [mid TO e}
        stack.append((mid_floor, e))   # pravé
        stack.append((s, mid_floor))   # levé

        time.sleep(throttle_s)

    # Výstup srovnej podle startu
    result.sort(key=lambda t: t[0])
    return result


def dedupe_day_output(day_raw_path: str, main_jsonl_path: str | None, day_dedup_path: str):
    """
    Dedup podle record `id` v rámci dne a (pokud je zadaný) i proti hlavnímu JSONL.
    Záznamy bez `id` přeskočíme.
    """
    seen_ids = set()

    # 1) Načti existující id z hlavního dumpu
    if main_jsonl_path and os.path.exists(main_jsonl_path):
        with open(main_jsonl_path, "r", encoding="utf-8") as base:
            for line in base:
                try:
                    rid = json.loads(line).get("id")
                    if rid is not None:
                        seen_ids.add(rid)
                except Exception:
                    continue

    # 2) Dedup v rámci dne
    in_count, out_count = 0, 0
    with open(day_raw_path, "r", encoding="utf-8") as inp, \
         open(day_dedup_path, "w", encoding="utf-8") as outp:
        for line in inp:
            in_count += 1
            try:
                rec = json.loads(line)
            except Exception:
                continue
            rid = rec.get("id")
            if rid is None:
                continue
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
            outp.write(line)
            out_count += 1

    return in_count, out_count


def main():
    ap = argparse.ArgumentParser(description="Harvest a single heavy day from Zenodo (datasets).")
    ap.add_argument("--day", required=True, help="Den v UTC, formát YYYY-MM-DD (např. 2023-12-28)")
    ap.add_argument("--out-dir", required=True, help="Výstupní adresář")
    ap.add_argument("--main-jsonl", default=None, help="Cesta k hlavnímu records.jsonl pro deduplikaci (volitelné)")
    ap.add_argument("--size", type=int, default=100, help="Velikost stránky pro Zenodo API (<= 1000)")
    ap.add_argument("--throttle-per-minute", type=int, default=55, help="Max počet požadavků za minutu")
    ap.add_argument("--hard-cap", type=int, default=9000, help="Max záznamů na sub-okno před dalším dělením")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    # Den [start, end) — tedy end je další půlnoc (pravá EXKLUZIVNÍ hranice)
    try:
        day_dt = datetime.strptime(args.day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        print("ERROR: --day musí být ve formátu YYYY-MM-DD", file=sys.stderr)
        sys.exit(2)

    day_start = day_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = (day_start + timedelta(days=1))  # exkluzivní horní hranice

    raw_path = os.path.join(args.out_dir, f"{args.day}_raw.jsonl")
    dedup_path = os.path.join(args.out_dir, f"{args.day}_dedup.jsonl")

    throttle_s = max(0.0, 60.0 / float(args.throttle_per_minute))

    with requests.Session() as sess:
        # 1) Vypočti sub-okna s pravou exkluzivní hranicí
        print(f"[1/4] Buduji sub-okna pro {args.day} (created, UTC), hard_cap={args.hard_cap} …")
        windows = split_until_cap(sess, day_start, day_end, args.hard_cap, throttle_s)
        # Odhad počtu (jen informativně)
        total_est = 0
        for s, e in windows:
            rng_q = build_range("created", iso_z(s), iso_z(e), True, False)
            q = f"(resource_type.type:dataset) AND {rng_q}"
            total_est += get_total(sess, q)
            time.sleep(throttle_s)
        print(f"     Oken: {len(windows)}, odhad celkem: {total_est}")

        # 2) Harvest všech oken do RAW JSONL
        print(f"[2/4] Stahuju okna do {raw_path} …")
        with open(raw_path, "w", encoding="utf-8") as out_fh:
            for s, e in windows:
                rng_q = build_range("created", iso_z(s), iso_z(e), True, False)  # [s TO e}
                q = f"(resource_type.type:dataset) AND {rng_q}"
                print(f"     Okno {iso_z(s)} .. {iso_z(e)}")
                fetch_window(sess, q, args.size, throttle_s, out_fh)

    # 3) Dedup proti main JSONL a v rámci dne
    print(f"[3/4] Deduplikuju vůči main JSONL → {dedup_path} …")
    in_count, out_count = dedupe_day_output(raw_path, args.main_jsonl, dedup_path)
    print(f"     RAW řádků: {in_count}  →  po dedup: {out_count}")

    # 4) Rychlá sanity
    try:
        distinct = set()
        kept = 0
        with open(dedup_path, "r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    rid = json.loads(line).get("id")
                    if rid is not None:
                        distinct.add(rid)
                    kept += 1
                except Exception:
                    continue
        print(f"[4/4] Sanity: v dedup souboru lines={kept}, distinct_ids={len(distinct)}")
    except Exception as e:
        print(f"[4/4] Sanity check failed: {e}", file=sys.stderr)

    print("Hotovo.")
    print(f"RAW:    {raw_path}")
    print(f"DEDUP.: {dedup_path}")


if __name__ == "__main__":
    main()

