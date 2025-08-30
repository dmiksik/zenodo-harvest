#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
harvest_day.py — Harvest a single heavy day from Zenodo (datasets, all_versions=false)
by recursively splitting the day into smaller time windows until each window
has <= --hard-cap results. Uses created timestamps (UTC), inclusive ranges
with the upper bound shifted back by 1 millisecond to avoid overlaps.

Usage:
    python harvest_day.py --day 2023-12-28 --out-dir ./zenodo_days \
      --main-jsonl zenodo_dump/raw/records.jsonl \
      --size 500 --throttle-per-minute 50 --hard-cap 9000
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
import requests
import math
from requests.exceptions import RequestException

Z_API = "https://zenodo.org/api/records"


def iso_z(dt: datetime) -> str:
    """Return ISO8601 timestamp in UTC ending with 'Z'."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_inclusive_range(field: str, start_iso: str, end_iso: str) -> str:
    """
    Inclusive Lucene range with quoted timestamps:
        field:["start" TO "end"]
    """
    return f'{field}:["{start_iso}" TO "{end_iso}"]'


def extract_total(data: dict) -> int:
    """Safely extract total from Zenodo response (int or {'value': int})."""
    hits = data.get("hits", {})
    total = hits.get("total", 0)
    if isinstance(total, dict):
        total = total.get("value", 0)
    try:
        return int(total)
    except Exception:
        return 0


def get_total(session: requests.Session, q: str, all_versions: bool = False) -> int:
    """Return only the count for given query. Uses size=1 (size=0 is invalid here)."""
    params = {
        "q": q,
        "all_versions": "true" if all_versions else "false",
        "size": 1,
    }
    r = session.get(Z_API, params=params, timeout=60)
    r.raise_for_status()
    return extract_total(r.json())


def request_with_retries(session, url, params, timeout, max_retries=6, throttle_s=0.0):
    """
    GET s retry/backoff. Reaguje na 5xx, 429 a síťové chyby.
    Při 429 respektuje Retry-After (pokud je).
    """
    attempt = 0
    while True:
        try:
            r = session.get(url, params=params, timeout=timeout)
            # 429: Too Many Requests → respektuj Retry-After
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else max(5.0, 2.0 ** attempt)
                time.sleep(wait)
                attempt += 1
                if attempt > max_retries:
                    r.raise_for_status()
                continue
            # 5xx: backoff
            if 500 <= r.status_code < 600:
                wait = max(3.0, 2.0 ** attempt)
                time.sleep(wait)
                attempt += 1
                if attempt > max_retries:
                    r.raise_for_status()
                continue
            # 2xx → hotovo
            r.raise_for_status()
            return r
        except RequestException:
            wait = max(3.0, 2.0 ** attempt)
            time.sleep(wait)
            attempt += 1
            if attempt > max_retries:
                raise

def fetch_window(session: requests.Session, q: str, size: int, throttle_s: float, out_fh):
    """
    Projede stránky pevného okna. Při 5xx/429/netechnických výpadcích
    opakuje požadavek s backoffem. Při opakovaných chybách okno dočasně
    zmenší `size` (nejnižší 50).
    """
    page = 1
    current_size = size
    while True:
        params = {
            "q": q,
            "all_versions": "false",
            "size": current_size,
            "page": page,
            "sort": "mostrecent",
        }
        try:
            r = request_with_retries(session, Z_API, params, timeout=120, max_retries=6, throttle_s=throttle_s)
            data = r.json()
            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                break
            for rec in hits:
                out_fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            page += 1
            time.sleep(throttle_s)
            # když jsme uspěli, můžeme zkusit vrátit size k původní hodnotě
            if current_size < size:
                current_size = min(size, current_size * 2)
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status and 500 <= status < 600:
                # po tvrdé 5xx chybě ještě jednou zkusíme s menší stránkou
                current_size = max(50, current_size // 2)
                continue
            raise


def split_until_cap(session: requests.Session,
                    start: datetime, end_exclusive: datetime,
                    hard_cap: int, throttle_s: float):
    """
    Recursively split the day [start, end) into sub-windows until each has <= hard_cap.
    Each query uses INCLUSIVE range, but the upper bound is (end - 1 ms), so adjacent
    windows don't overlap. Returns list of (start_dt, end_exclusive_dt).
    """
    stack = [(start, end_exclusive)]
    result = []
    one_ms = timedelta(microseconds=1000)

    while stack:
        s, e_excl = stack.pop()

        s_iso = iso_z(s)
        e_inc_iso = iso_z(e_excl - one_ms)  # inclusive upper bound is end-1ms
        rng_q = build_inclusive_range("created", s_iso, e_inc_iso)
        q = f"(resource_type.type:dataset) AND {rng_q}"
        total = get_total(session, q)

        if total <= hard_cap:
            result.append((s, e_excl))
            time.sleep(throttle_s)
            continue

        span = (e_excl - s).total_seconds()
        if span <= 1:
            # If we got here, the window is tiny but still > hard_cap; accept anyway
            result.append((s, e_excl))
            time.sleep(throttle_s)
            continue

        # Split in half
        half_seconds = max(1, int(span // 2))
        mid = s + timedelta(seconds=half_seconds)
        if mid <= s:
            mid = s + timedelta(seconds=1)
        if mid >= e_excl:
            mid = e_excl - timedelta(seconds=1)

        # Two sub-windows: [s, mid) and [mid, e_excl)
        stack.append((mid, e_excl))  # right
        stack.append((s, mid))       # left

        time.sleep(throttle_s)

    result.sort(key=lambda t: t[0])
    return result


def dedupe_day_output(day_raw_path: str, main_jsonl_path: str | None, day_dedup_path: str):
    """
    Deduplicate by record 'id' within the day and (if provided) against the main JSONL.
    Records without 'id' are skipped.
    """
    seen_ids = set()

    # Against the main dump
    if main_jsonl_path and os.path.exists(main_jsonl_path):
        with open(main_jsonl_path, "r", encoding="utf-8") as base:
            for line in base:
                try:
                    rid = json.loads(line).get("id")
                    if rid is not None:
                        seen_ids.add(rid)
                except Exception:
                    continue

    # Within the day
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
    ap.add_argument("--day", required=True, help="UTC day, format YYYY-MM-DD (e.g., 2023-12-28)")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--main-jsonl", default=None, help="Path to main records.jsonl for deduplication (optional)")
    ap.add_argument("--size", type=int, default=100, help="Zenodo API page size (<= 1000)")
    ap.add_argument("--throttle-per-minute", type=int, default=55, help="Max requests per minute")
    ap.add_argument("--hard-cap", type=int, default=9000, help="Max records per sub-window before further split")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    # Day interval [start, end) — end is next midnight (exclusive)
    try:
        day_dt = datetime.strptime(args.day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        print("ERROR: --day must be in format YYYY-MM-DD", file=sys.stderr)
        sys.exit(2)

    day_start = day_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end_excl = day_start + timedelta(days=1)  # exclusive upper bound

    raw_path = os.path.join(args.out_dir, f"{args.day}_raw.jsonl")
    dedup_path = os.path.join(args.out_dir, f"{args.day}_dedup.jsonl")

    throttle_s = max(0.0, 60.0 / float(args.throttle_per_minute))

    with requests.Session() as sess:
        print(f"[1/4] Building sub-windows for {args.day} (created, UTC), hard_cap={args.hard_cap} …")
        windows = split_until_cap(sess, day_start, day_end_excl, args.hard_cap, throttle_s)

        # Informative estimate
        total_est = 0
        one_ms = timedelta(microseconds=1000)
        for s, e_excl in windows:
            s_iso = iso_z(s)
            e_inc_iso = iso_z(e_excl - one_ms)
            rng_q = build_inclusive_range("created", s_iso, e_inc_iso)
            q = f"(resource_type.type:dataset) AND {rng_q}"
            total_est += get_total(sess, q)
            time.sleep(throttle_s)
        print(f"     Windows: {len(windows)}, estimated total: {total_est}")

        print(f"[2/4] Harvesting windows into {raw_path} …")
        with open(raw_path, "w", encoding="utf-8") as out_fh:
            for s, e_excl in windows:
                s_iso = iso_z(s)
                e_inc_iso = iso_z(e_excl - one_ms)  # inclusive end
                rng_q = build_inclusive_range("created", s_iso, e_inc_iso)
                q = f"(resource_type.type:dataset) AND {rng_q}"
                print(f"     Window {s_iso} .. {e_inc_iso}")
                fetch_window(sess, q, args.size, throttle_s, out_fh)

    print(f"[3/4] Deduplicating vs. main JSONL → {dedup_path} …")
    in_count, out_count = dedupe_day_output(raw_path, args.main_jsonl, dedup_path)
    print(f"     RAW lines: {in_count}  →  after dedup: {out_count}")

    # Quick sanity: distinct ids in dedup
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
        print(f"[4/4] Sanity: dedup file lines={kept}, distinct_ids={len(distinct)}")
    except Exception as e:
        print(f"[4/4] Sanity check failed: {e}", file=sys.stderr)

    print("Done.")
    print(f"RAW:    {raw_path}")
    print(f"DEDUP.: {dedup_path}")


if __name__ == "__main__":
    main()

