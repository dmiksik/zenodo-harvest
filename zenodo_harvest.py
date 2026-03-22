#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Zenodo datasets harvester (resumable, robust splitting)

Main improvements compared to the original script:
- uses a documented sort value (`-mostrecent`) instead of `oldest`
- respects current Zenodo Search API limits (size/request-rate)
- supports optional API token (`--api-token` or ZENODO_API_TOKEN)
- starts cleanly on a fresh run (removes stale state unless `--append` is used)
- avoids recursive retries in `get_total()`
- protects against infinitely fine time-window splitting
- can preload existing record IDs when resuming with `--dedupe-in-memory`
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import time
from collections import deque
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm

ZENODO_API = "https://zenodo.org/api/records"
DEFAULT_TIMEOUT = 60
ONE_MS = timedelta(milliseconds=1)
MIN_SPLIT_DURATION = timedelta(seconds=1)
ANON_MAX_PAGE_SIZE = 25
AUTH_MAX_PAGE_SIZE = 100
MAX_REQUESTS_PER_MINUTE = 30


# ----------------- utils -----------------

def iso_z(dt: datetime) -> str:
    """Return ISO8601 UTC with 'Z', preserving microseconds when present."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    if dt.microsecond:
        return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_date_strict(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Invalid date '{s}'. Expected YYYY-MM-DD.") from e


def parse_iso_datetime(s: str) -> datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def build_session(api_token: Optional[str]) -> requests.Session:
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({"Accept": "application/json"})
    if api_token:
        s.headers.update({"Authorization": f"Bearer {api_token}"})
    return s


def safe_json_dump(path: Path, data: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            return default


def write_jsonl(path: Path, docs: List[Dict[str, Any]]) -> None:
    with path.open("a", encoding="utf-8") as f:
        for d in docs:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")


def iter_existing_ids(path: Path) -> Iterable[str]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                logging.warning("Skipping invalid JSONL line %d in %s", line_no, path)
                continue
            rec_id = rec.get("id")
            if rec_id is not None:
                yield str(rec_id)


def clamp_page_size(requested_size: int, authenticated: bool) -> int:
    max_size = AUTH_MAX_PAGE_SIZE if authenticated else ANON_MAX_PAGE_SIZE
    if requested_size > max_size:
        logging.warning(
            "Requested page size %d exceeds current Zenodo limit %d; using %d instead.",
            requested_size, max_size, max_size,
        )
        return max_size
    if requested_size < 1:
        logging.warning("Requested page size %d is invalid; using 1 instead.", requested_size)
        return 1
    return requested_size


def clamp_rate(requests_per_minute: int) -> int:
    if requests_per_minute > MAX_REQUESTS_PER_MINUTE:
        logging.warning(
            "Requested rate %d/min exceeds current Zenodo limit %d/min; using %d/min instead.",
            requests_per_minute, MAX_REQUESTS_PER_MINUTE, MAX_REQUESTS_PER_MINUTE,
        )
        return MAX_REQUESTS_PER_MINUTE
    if requests_per_minute < 1:
        logging.warning("Requested rate %d/min is invalid; using 1/min instead.", requests_per_minute)
        return 1
    return requests_per_minute


def request_with_retries(
    sess: requests.Session,
    params: Dict[str, Any],
    *,
    timeout_s: int = DEFAULT_TIMEOUT,
    max_attempts: int = 8,
    transient_sleep_s: float = 3.0,
) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = sess.get(ZENODO_API, params=params, timeout=timeout_s)
        except requests.RequestException as e:
            last_exc = e
            logging.warning("Request exception (attempt %d/%d): %s", attempt, max_attempts, e)
            time.sleep(min(transient_sleep_s * attempt, 20))
            continue

        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "5"))
            logging.info("429 received. Sleeping %ss before retry (%d/%d).", wait, attempt, max_attempts)
            time.sleep(wait)
            continue

        if r.status_code >= 500:
            logging.info("HTTP %d received. Sleeping %.1fs before retry (%d/%d).", r.status_code, transient_sleep_s, attempt, max_attempts)
            time.sleep(min(transient_sleep_s * attempt, 20))
            continue

        return r

    if last_exc is not None:
        raise RuntimeError(f"Request failed after {max_attempts} attempts: {last_exc}") from last_exc
    raise RuntimeError(f"Request failed after {max_attempts} attempts with repeated HTTP 429/5xx responses.")


def get_total(sess: requests.Session, params: Dict[str, Any]) -> int:
    p = dict(params)
    p["size"] = 1
    p["page"] = 1
    r = request_with_retries(sess, p)
    r.raise_for_status()
    data = r.json()
    total = data.get("hits", {}).get("total", 0)
    if isinstance(total, dict):
        total = total.get("value", 0)
    try:
        return int(total)
    except Exception:
        return 0


def choose_sort_for_field(field: str) -> str:
    # Documented API syntax: `mostrecent` or `-mostrecent`
    # For harvesting we want chronological order (oldest first).
    return "-mostrecent" if field in {"created", "updated"} else "mostrecent"


def inclusive_range_q(field_iso_left: str, field_iso_right_excl: str, field: str) -> str:
    """
    Build an inclusive query from an exclusive upper bound by subtracting 1 ms.
    """
    start_iso = field_iso_left
    end_dt = parse_iso_datetime(field_iso_right_excl) - ONE_MS
    end_iso_incl = iso_z(end_dt)
    return f'{field}:["{start_iso}" TO "{end_iso_incl}"]'


# ----------------- windows -----------------

@dataclass
class Window:
    start_iso: str
    end_excl_iso: str
    est_total: int = 0
    page: int = 1
    size: int = 25

    def duration(self) -> timedelta:
        return parse_iso_datetime(self.end_excl_iso) - parse_iso_datetime(self.start_iso)

    def can_split(self, min_duration: timedelta = MIN_SPLIT_DURATION) -> bool:
        return self.duration() > min_duration

    def split_mid(self, min_duration: timedelta = MIN_SPLIT_DURATION) -> tuple["Window", "Window"]:
        s = parse_iso_datetime(self.start_iso)
        e = parse_iso_datetime(self.end_excl_iso)
        dur = e - s
        if dur <= min_duration:
            raise ValueError(f"Cannot split window shorter than or equal to {min_duration}: {self.start_iso}..{self.end_excl_iso}")
        mid = s + dur / 2
        if mid <= s or mid >= e:
            raise ValueError(f"Degenerate split for window {self.start_iso}..{self.end_excl_iso}")
        left = Window(iso_z(s), iso_z(mid), 0, 1, self.size)
        right = Window(iso_z(mid), iso_z(e), 0, 1, self.size)
        return left, right


def plan_windows_by_count(
    sess: requests.Session,
    field: str,
    start_dt: datetime,
    end_excl_dt: datetime,
    hard_cap: int,
    base_params: Dict[str, Any],
    throttle_s: float,
    min_split_duration: timedelta = MIN_SPLIT_DURATION,
) -> List[Window]:
    """
    Recursively split [start, end) until each window has <= hard_cap results,
    unless the window is already too short to split safely.
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
        pbar.set_postfix_str(f"{w.start_iso[:19]}..{w.end_excl_iso[:19]} ~{total}")

        if total <= hard_cap or not w.can_split(min_split_duration):
            if total > hard_cap and not w.can_split(min_split_duration):
                logging.warning(
                    "Keeping dense unsplittable window %s..%s with est_total=%d (> hard_cap=%d).",
                    w.start_iso, w.end_excl_iso, total, hard_cap,
                )
            out.append(w)
        else:
            left, right = w.split_mid(min_split_duration)
            # stack is LIFO, so push right first to process left next
            stack.extend([
                (parse_iso_datetime(right.start_iso), parse_iso_datetime(right.end_excl_iso)),
                (parse_iso_datetime(left.start_iso), parse_iso_datetime(left.end_excl_iso)),
            ])
        time.sleep(throttle_s)

    pbar.close()
    out.sort(key=lambda W: W.start_iso)
    return out


# ----------------- harvesting -----------------

@dataclass
class HarvestState:
    windows: List[Dict[str, Any]]

    @classmethod
    def load(cls, path: Path) -> "HarvestState":
        data = read_json(path, {"windows": []})
        return HarvestState(windows=data.get("windows", []))

    def save(self, path: Path) -> None:
        safe_json_dump(path, {"windows": self.windows})


def fetch_page(sess: requests.Session, params: Dict[str, Any], timeout_s: int = DEFAULT_TIMEOUT) -> requests.Response:
    return request_with_retries(sess, params, timeout_s=timeout_s, max_attempts=8, transient_sleep_s=3.0)


def initialize_run(raw_path: Path, state_path: Path, append: bool) -> None:
    if append:
        return
    if raw_path.exists():
        logging.info("Fresh run requested: removing existing output file %s", raw_path)
        raw_path.unlink()
    if state_path.exists():
        logging.info("Fresh run requested: removing stale state file %s", state_path)
        state_path.unlink()


def harvest(
    out_dir: Path,
    sess: requests.Session,
    field: str,
    windows: List[Window],
    size: int,
    throttle_per_minute: int,
    hard_cap: int,
    base_params: Dict[str, Any],
    state_path: Path,
    append: bool,
    dedupe_in_memory: bool,
):
    raw_path = out_dir / "records.jsonl"
    initialize_run(raw_path, state_path, append)

    state = HarvestState.load(state_path)
    queue: Deque[Window] = deque()

    if state.windows:
        for w in state.windows:
            queue.append(Window(**w))
        logging.info("Loaded %d queued windows from state file %s", len(queue), state_path)
    else:
        for w in windows:
            w.size = size
            queue.append(w)

    time_per_req = 60.0 / max(1, throttle_per_minute)
    pbar = tqdm(total=len(queue), desc="Harvesting (windows)", unit="win")

    seen_ids: Optional[set[str]] = set() if dedupe_in_memory else None
    if seen_ids is not None and append and raw_path.exists():
        logging.info("Preloading existing IDs from %s for in-memory deduplication...", raw_path)
        seen_ids.update(iter_existing_ids(raw_path))
        logging.info("Preloaded %d existing IDs.", len(seen_ids))

    while queue:
        w = queue[0]
        sort = choose_sort_for_field(field)
        q = inclusive_range_q(w.start_iso, w.end_excl_iso, field)

        if w.est_total <= 0:
            w.est_total = get_total(sess, dict(base_params, sort=sort, q=q))
        max_pages = max(1, math.ceil(w.est_total / max(1, w.size)) + 2)

        logging.info(
            "Window %s..%s ~%d (page=%d, size=%d)",
            w.start_iso, w.end_excl_iso, w.est_total, w.page, w.size,
        )
        page_pbar = tqdm(
            total=max_pages,
            initial=max(0, w.page - 1),
            desc=f"  Pages {w.start_iso[:19]}..{w.end_excl_iso[:19]}",
            unit="page",
            leave=False,
        )

        consecutive_5xx = 0
        last_progress = time.time()
        requeued = False

        while w.page <= max_pages:
            params = dict(base_params, sort=sort, size=w.size, page=w.page, q=q)
            try:
                r = fetch_page(sess, params)
            except Exception as e:
                logging.warning("Persistent request failure at page %d: %s", w.page, e)
                if w.can_split():
                    left, right = w.split_mid()
                    logging.warning("Auto-splitting window %s..%s after repeated failures", w.start_iso, w.end_excl_iso)
                    queue.popleft()
                    queue.appendleft(right)
                    queue.appendleft(left)
                    state.windows = [asdict(x) for x in queue]
                    state.save(state_path)
                    requeued = True
                else:
                    raise
                break

            rl_rem = r.headers.get("X-RateLimit-Remaining")
            rl_lim = r.headers.get("X-RateLimit-Limit")
            rl_res = r.headers.get("X-RateLimit-Reset")

            if r.status_code >= 500:
                consecutive_5xx += 1
                logging.info("5xx on page %d (try %d).", w.page, consecutive_5xx)
                time.sleep(3)
                if consecutive_5xx >= 8 or (time.time() - last_progress) > 300:
                    if w.can_split():
                        left, right = w.split_mid()
                        logging.warning("Auto-splitting window %s..%s due to persistent 5xx", w.start_iso, w.end_excl_iso)
                        queue.popleft()
                        queue.appendleft(right)
                        queue.appendleft(left)
                        state.windows = [asdict(x) for x in queue]
                        state.save(state_path)
                        requeued = True
                        break
                    raise RuntimeError(f"Persistent 5xx on unsplittable window {w.start_iso}..{w.end_excl_iso}")
                continue

            if r.status_code == 400:
                if w.size > 1:
                    new_size = max(1, w.size // 2)
                    if new_size < w.size:
                        w.size = new_size
                        logging.warning("400 on page %d. Reducing page size to %d and retrying.", w.page, w.size)
                        queue[0] = w
                        state.windows = [asdict(x) for x in queue]
                        state.save(state_path)
                        time.sleep(time_per_req)
                        continue
                if w.can_split():
                    left, right = w.split_mid()
                    logging.warning("400 on page %d. Splitting window %s..%s", w.page, w.start_iso, w.end_excl_iso)
                    queue.popleft()
                    queue.appendleft(right)
                    queue.appendleft(left)
                    state.windows = [asdict(x) for x in queue]
                    state.save(state_path)
                    requeued = True
                    break
                raise RuntimeError(f"HTTP 400 on unsplittable window {w.start_iso}..{w.end_excl_iso}")

            r.raise_for_status()
            items = r.json().get("hits", {}).get("hits", [])
            n = len(items)
            logging.info(
                "win %s..%s page %d: %d items (rate %s/%s reset %s)",
                w.start_iso, w.end_excl_iso, w.page, n, rl_rem, rl_lim, rl_res,
            )

            if n == 0:
                break

            if seen_ids is not None:
                out_items = []
                for it in items:
                    rec_id = it.get("id")
                    rec_id = None if rec_id is None else str(rec_id)
                    if rec_id is not None and rec_id in seen_ids:
                        continue
                    if rec_id is not None:
                        seen_ids.add(rec_id)
                    out_items.append(it)
                if out_items:
                    write_jsonl(raw_path, out_items)
            else:
                write_jsonl(raw_path, items)

            w.page += 1
            consecutive_5xx = 0
            last_progress = time.time()

            queue[0] = w
            state.windows = [asdict(x) for x in queue]
            state.save(state_path)
            page_pbar.update(1)
            time.sleep(time_per_req)

        page_pbar.close()

        if requeued:
            continue

        if queue and queue[0] is w:
            queue.popleft()
            state.windows = [asdict(x) for x in queue]
            state.save(state_path)
            pbar.update(1)

    pbar.close()
    logging.info("Harvesting finished. RAW at %s", raw_path)


# ----------------- main -----------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Zenodo datasets harvester (resumable, robust splitting)")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--size", type=int, default=100, help="Requested page size; will be clamped to current Zenodo limits.")
    ap.add_argument("--throttle-per-minute", type=int, default=30, help="Requested rate; will be clamped to current Zenodo limits.")
    ap.add_argument("--window-field", choices=["created", "updated"], default="created")
    ap.add_argument("--date-from", required=True, type=parse_date_strict)
    ap.add_argument("--date-to", default=None, type=parse_date_strict)
    ap.add_argument("--hard-cap", type=int, default=9000)
    ap.add_argument("--append", action="store_true", help="Resume/continue using existing records.jsonl and state.json.")
    ap.add_argument("--state-file", default=None)
    ap.add_argument("--dedupe-in-memory", action="store_true", help="Keep an in-memory set(id) to avoid duplicate writes.")
    ap.add_argument("--api-token", default=os.environ.get("ZENODO_API_TOKEN"), help="Zenodo API token. Can also be supplied via ZENODO_API_TOKEN.")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    state_path = Path(args.state_file) if args.state_file else out_dir / "state.json"

    authenticated = bool(args.api_token)
    size = clamp_page_size(args.size, authenticated)
    throttle_per_minute = clamp_rate(args.throttle_per_minute)

    if authenticated:
        logging.info("Using authenticated requests.")
    else:
        logging.info("Using anonymous requests.")

    sess = build_session(args.api_token)

    start = args.date_from
    if args.date_to:
        end_excl = args.date_to + timedelta(days=1)
    else:
        end_excl = datetime.now(timezone.utc).date() + timedelta(days=1)

    field = args.window_field
    base_params = {
        "type": "dataset",
        "all_versions": 0,
        "status": "published",
    }

    windows = plan_windows_by_count(
        sess=sess,
        field=field,
        start_dt=datetime(start.year, start.month, start.day, tzinfo=timezone.utc),
        end_excl_dt=datetime(end_excl.year, end_excl.month, end_excl.day, tzinfo=timezone.utc),
        hard_cap=args.hard_cap,
        base_params=base_params,
        throttle_s=max(0.0, 60.0 / float(throttle_per_minute)),
        min_split_duration=MIN_SPLIT_DURATION,
    )

    logging.info("Planned %d windows.", len(windows))

    harvest(
        out_dir=out_dir,
        sess=sess,
        field=field,
        windows=windows,
        size=size,
        throttle_per_minute=throttle_per_minute,
        hard_cap=args.hard_cap,
        base_params=base_params,
        state_path=state_path,
        append=args.append,
        dedupe_in_memory=args.dedupe_in_memory,
    )

    logging.info("All done. Outputs in: %s", out_dir)


if __name__ == "__main__":
    main()
