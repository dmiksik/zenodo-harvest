#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Zenodo datasets harvester (resumable, robust splitting)
- window-field=created -> sort=oldest (stable paging)
- status=published filter applied to all API requests
- split windows always while total > hard_cap (even single-day -> halves/hours)
- on HTTP 400 split window (do not stop early)
- rate-limit friendly; 429/5xx retries with backoff
- writes RAW JSONL and state.json (resumable)
"""

from __future__ import annotations
import argparse, json, math, time, sys, os, logging
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone, date
from typing import List, Dict, Any, Deque, Tuple
from collections import deque

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dateutil import parser as dateparser
from tqdm import tqdm

ZENODO_API = "https://zenodo.org/api/records"
ONE_MS = timedelta(milliseconds=1)

# ----------------- utils -----------------

def iso_z(dt: datetime) -> str:
    """Return ISO8601 UTC with 'Z'."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_date(s: str) -> date:
    return dateparser.parse(s).date()

def build_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=6, backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",), raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter); s.mount("https://", adapter)
    s.headers.update({"Accept": "application/json"})
    return s

def safe_json_dump(path: str, data: Any):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def read_json(path: str, default):
    if not os.path.exists(path): return default
    with open(path, "r", encoding="utf-8") as f:
        try: return json.load(f)
        except Exception: return default

def write_jsonl(path: str, docs: List[Dict[str, Any]]):
    with open(path, "a", encoding="utf-8") as f:
        for d in docs:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

def get_total(sess: requests.Session, params: Dict[str, Any]) -> int:
    p = dict(params); p["size"] = 1; p["page"] = 1
    r = sess.get(ZENODO_API, params=p, timeout=60)
    if r.status_code == 429:
        wait = int(r.headers.get("Retry-After", "5")); time.sleep(wait)
        return get_total(sess, params)
    if r.status_code >= 500:
        time.sleep(3); return get_total(sess, params)
    r.raise_for_status()
    data = r.json()
    total = data.get("hits", {}).get("total", 0)
    if isinstance(total, dict): total = total.get("value", 0)
    try: return int(total)
    except Exception: return 0

def choose_sort_for_field(field: str) -> str:
    # Stable paging for created; others keep mostrecent
    return "oldest" if field == "created" else "mostrecent"

def inclusive_range_q(field_iso_left: str, field_iso_right_excl: str, field: str) -> str:
    """
    Use inclusive query bounds but subtract 1ms from exclusive right bound:
      field:["start_iso" TO "end_excl - 1ms"]
    """
    start_iso = field_iso_left
    end_iso_incl = (dateparser.parse(field_iso_right_excl) - ONE_MS).strftime("%Y-%m-%dT%H:%M:%SZ")
    return f'{field}:["{start_iso}" TO "{end_iso_incl}"]'

# ----------------- windows -----------------

@dataclass
class Window:
    start_iso: str       # inclusive left (ISO UTC 'Z')
    end_excl_iso: str    # exclusive right (ISO UTC 'Z')
    est_total: int = 0
    page: int = 1
    size: int = 250
    done: bool = False

    def duration(self) -> timedelta:
        s = dateparser.parse(self.start_iso)
        e = dateparser.parse(self.end_excl_iso)
        return e - s

    def split_mid(self) -> tuple['Window','Window']:
        s = dateparser.parse(self.start_iso)
        e = dateparser.parse(self.end_excl_iso)
        mid = s + (e - s) / 2
        left = Window(iso_z(s), iso_z(mid), 0, 1, self.size, False)
        right = Window(iso_z(mid), iso_z(e), 0, 1, self.size, False)
        return left, right

def plan_windows_by_count(sess: requests.Session, field: str,
                          start_dt: datetime, end_excl_dt: datetime,
                          hard_cap: int, base_params: Dict[str, Any],
                          throttle_s: float) -> List[Window]:
    """
    Recursively split [start, end) until each window has <= hard_cap results.
    Always split while total > hard_cap (even for 1-day windows).
    """
    out: List[Window] = []
    stack: List[tuple[datetime, datetime]] = [(start_dt, end_excl_dt)]
    sort = choose_sort_for_field(field)

    pbar = tqdm(total=1, desc=f"Counting windows ({field})", unit="win")
    processed = 0

    while stack:
        s, e_excl = stack.pop()
        w = Window(iso_z(s), iso_z(e_excl))
        q = inclusive_range_q(w.start_iso, w.end_excl_iso, field)
        params = dict(base_params, sort=sort, q=q)

        total = get_total(sess, params)
        w.est_total = total
        processed += 1
        pbar.total = processed + len(stack)
        pbar.update(1)
        pbar.set_postfix_str(f"{w.start_iso[:10]}..{w.end_excl_iso[:10]} ~{total}")

        if total <= hard_cap:
            out.append(w)
        else:
            # always split (even sub-day)
            mid = s + (e_excl - s) / 2
            left = (s, mid)
            right = (mid, e_excl)
            stack.extend([right, left])
        time.sleep(throttle_s)

    pbar.close()
    out.sort(key=lambda W: W.start_iso)
    return out

# ----------------- harvesting -----------------

@dataclass
class HarvestState:
    windows: List[Dict[str, Any]]

    @classmethod
    def load(cls, path: str) -> 'HarvestState':
        data = read_json(path, {"windows": []})
        return HarvestState(windows=data.get("windows", []))

    def save(self, path: str):
        safe_json_dump(path, {"windows": self.windows})

def fetch_page(sess: requests.Session, params: Dict[str, Any], timeout_s=60) -> requests.Response:
    return sess.get(ZENODO_API, params=params, timeout=timeout_s)

def harvest(out_dir: str, sess: requests.Session, field: str,
            windows: List[Window], size: int, throttle_per_minute: int,
            hard_cap: int, base_params: Dict[str, Any],
            state_path: str, append: bool, dedupe_in_memory: bool):

    raw_path = os.path.join(out_dir, "records.jsonl")
    if not append and os.path.exists(raw_path):
        os.remove(raw_path)

    state = HarvestState.load(state_path)
    queue: Deque[Window] = deque()

    if state.windows:
        for w in state.windows: queue.append(Window(**w))
    else:
        for w in windows:
            w.size = size
            queue.append(w)

    time_per_req = 60.0 / max(1, throttle_per_minute)
    pbar = tqdm(total=len(queue), desc="Harvesting (windows)", unit="win")

    seen_ids: set[str] = set() if dedupe_in_memory else None

    while queue:
        w = queue[0]
        sort = choose_sort_for_field(field)
        q = inclusive_range_q(w.start_iso, w.end_excl_iso, field)

        if w.est_total <= 0:
            w.est_total = get_total(sess, dict(base_params, sort=sort, q=q))
        max_pages = max(1, math.ceil(w.est_total / w.size) + 2)

        logging.info("Window %s..%s ~%d (page=%d,size=%d)",
                     w.start_iso, w.end_excl_iso, w.est_total, w.page, w.size)
        page_pbar = tqdm(total=max_pages, initial=w.page-1,
                         desc=f"  Pages {w.start_iso[:10]}..{w.end_excl_iso[:10]}",
                         unit="page", leave=False)

        consecutive_5xx = 0
        last_progress = time.time()

        while w.page <= max_pages:
            params = dict(base_params, sort=sort, size=w.size, page=w.page, q=q)
            try:
                r = fetch_page(sess, params)
            except requests.RequestException as e:
                logging.warning("Request exception p%d: %s (sleep 5s)", w.page, e)
                time.sleep(5); continue

            rl_rem = r.headers.get("X-RateLimit-Remaining")
            rl_lim = r.headers.get("X-RateLimit-Limit")
            rl_res = r.headers.get("X-RateLimit-Reset")

            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", "5"))
                logging.info("429 p%d. Retry-After=%ss (rate %s/%s reset %s)",
                             w.page, wait, rl_rem, rl_lim, rl_res)
                time.sleep(wait); continue

            if r.status_code >= 500:
                consecutive_5xx += 1
                logging.info("5xx p%d (try %d). Retry 3s", w.page, consecutive_5xx)
                time.sleep(3)
                if consecutive_5xx >= 8 or (time.time() - last_progress) > 300:
                    left, right = w.split_mid()
                    logging.warning("Auto-splitting window %s..%s due to persistent 5xx",
                                    w.start_iso, w.end_excl_iso)
                    queue.popleft()
                    queue.appendleft(right); queue.appendleft(left)
                    state.windows = [asdict(x) for x in queue]; state.save(state_path)
                    page_pbar.close()
                    break
                continue

            if r.status_code == 400:
                # split instead of stopping early
                if w.duration() <= timedelta(hours=1) and w.size > 50:
                    w.size = max(50, w.size // 2)
                    logging.warning("400 p%d on short window. Reducing size→%d and retrying.",
                                    w.page, w.size)
                    state.windows = [asdict(x) for x in queue]; state.save(state_path)
                    time.sleep(time_per_req); continue
                left, right = w.split_mid()
                logging.warning("400 p%d. Splitting window %s..%s",
                                w.page, w.start_iso, w.end_excl_iso)
                queue.popleft()
                queue.appendleft(right); queue.appendleft(left)
                state.windows = [asdict(x) for x in queue]; state.save(state_path)
                page_pbar.close()
                break

            r.raise_for_status()
            items = r.json().get("hits", {}).get("hits", [])
            n = len(items)
            logging.info("win %s..%s page %d: %d items (rate %s/%s reset %s)",
                         w.start_iso, w.end_excl_iso, w.page, n, rl_rem, rl_lim, rl_res)

            if n == 0:
                break

            if seen_ids is not None:
                out = []
                for it in items:
                    i = str(it.get("id"))
                    if i in seen_ids: continue
                    seen_ids.add(i)
                    out.append(it)
                if out:
                    write_jsonl(raw_path, out)
            else:
                write_jsonl(raw_path, items)

            w.page += 1
            consecutive_5xx = 0
            last_progress = time.time()

            queue[0] = w
            state.windows = [asdict(x) for x in queue]
            state.save(state_path)
            time.sleep(time_per_req)

        page_pbar.close()

        if queue and queue[0] is w:
            queue.popleft()
            state.windows = [asdict(x) for x in queue]
            state.save(state_path)
            pbar.update(1)

    pbar.close()
    logging.info("Harvesting finished. RAW at %s", raw_path)

# ----------------- main -----------------

def main():
    ap = argparse.ArgumentParser(description="Zenodo datasets harvester (resumable, robust splitting)")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--size", type=int, default=250)
    ap.add_argument("--throttle-per-minute", type=int, default=55)
    ap.add_argument("--window-field", choices=["created","updated"], default="created")
    ap.add_argument("--date-from", required=True)
    ap.add_argument("--date-to", default=None)
    ap.add_argument("--hard-cap", type=int, default=9000)
    ap.add_argument("--append", action="store_true")
    ap.add_argument("--state-file", default=None)
    ap.add_argument("--dedupe-in-memory", action="store_true",
                    help="Keep in-memory set(id) to avoid duplicate writes.")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s")

    os.makedirs(args.out_dir, exist_ok=True)
    state_path = args.state_file or os.path.join(args.out_dir, "state.json")

    sess = build_session()

    # Snapshot exclusive upper bound
    start = parse_date(args.date_from)
    if args.date_to:
        end_excl = parse_date(args.date_to) + timedelta(days=1)
    else:
        end_excl = datetime.now(timezone.utc).date() + timedelta(days=1)

    field = args.window_field
    base_params = {
        "type": "dataset",
        "all_versions": 0,
        "status": "published",   # <<<<<< added as requested
    }

    windows = plan_windows_by_count(
        sess=sess, field=field,
        start_dt=datetime(start.year, start.month, start.day, tzinfo=timezone.utc),
        end_excl_dt=datetime(end_excl.year, end_excl.month, end_excl.day, tzinfo=timezone.utc),
        hard_cap=args.hard_cap, base_params=base_params,
        throttle_s=max(0.0, 60.0/float(args.throttle_per_minute))
    )

    logging.info("Planned %d windows.", len(windows))

    harvest(
        out_dir=args.out_dir, sess=sess, field=field,
        windows=windows, size=args.size,
        throttle_per_minute=args.throttle_per_minute,
        hard_cap=args.hard_cap, base_params=base_params,
        state_path=state_path, append=args.append,
        dedupe_in_memory=args.dedupe_in_memory
    )

    logging.info("All done. Outputs in: %s", args.out_dir)

if __name__ == "__main__":
    main()
