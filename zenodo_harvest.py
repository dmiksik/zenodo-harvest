#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Zenodo datasets harvester (resumable, auto-splitting on 5xx)
- Vyhne se 10k limitu (časová okna) + při 5xx/timeout automaticky dělí aktuální okno
- Per-page logy, ukládání stavu (fronta oken + stránka + size)
- RAW JSONL → Parquet → DuckDB, volitelná shoda afiliací
pip install requests pandas duckdb pyarrow python-dateutil tqdm unidecode
"""

import json, math, time, argparse, logging, pathlib, re
from datetime import datetime, timedelta, timezone, date
from typing import List, Dict, Any, Optional, Tuple, Deque
from collections import deque

import requests
from requests.adapters import HTTPAdapter, Retry
import pandas as pd
import duckdb
from tqdm import tqdm
from unidecode import unidecode
from dateutil import parser as dateparser

ZENODO_API = "https://zenodo.org/api/records"

CZ_KEYWORDS = [
    "czech", "czechia", "czech republic", "česká republika", "česko",
    "praha", "prague", "brno", "ostrava", "plzeň", "pilsen",
    "olomouc", "hradec králové", "hradec kralove", "pardubice",
    "liberec", "usti", "ústí", "usti nad labem", "české budějovice",
    "ceske budejovice", "zlin", "zlín", "opava", "jihlava", "česk", "cesk",
]
CZ_REGEX = re.compile(r"(" + r"|".join(re.escape(w) for w in CZ_KEYWORDS) + r")", re.IGNORECASE | re.UNICODE)

# ---------- utils ----------
def build_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1.0,
                    status_forcelist=(429, 500, 502, 503, 504),
                    allowed_methods=("GET",), raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter); s.mount("https://", adapter)
    s.headers.update({"Accept": "application/json"})
    return s

def safe_mkdir(p: pathlib.Path): p.mkdir(parents=True, exist_ok=True)

def write_jsonl(path: pathlib.Path, docs: List[Dict[str, Any]]):
    with path.open("a", encoding="utf-8") as f:
        for d in docs: f.write(json.dumps(d, ensure_ascii=False) + "\n")

def norm_text(s: Optional[str]) -> str: return unidecode(s).lower().strip() if s else ""

def text_has_cz(text: Optional[str]) -> bool: return bool(text and CZ_REGEX.search(text))

def iso_date(d: datetime) -> str: return d.strftime("%Y-%m-%d")

def save_json(path: pathlib.Path, obj: Any):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def load_json(path: pathlib.Path, default: Any):
    if path.exists():
        try: return json.loads(path.read_text(encoding="utf-8"))
        except Exception: return default
    return default

# ---------- flatten ----------
def flatten_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    md = rec.get("metadata") or {}
    title = md.get("title"); description = md.get("description")
    keywords = md.get("keywords") or []; locations = md.get("locations") or []
    creators = md.get("creators") or []; contributors = md.get("contributors") or []
    text_blobs = []
    if isinstance(title, str): text_blobs.append(title)
    if isinstance(description, str): text_blobs.append(description)
    if isinstance(keywords, list): text_blobs.extend([k for k in keywords if isinstance(k, str)])
    for loc in locations:
        for k in ("place", "country", "description"):
            v = loc.get(k); 
            if isinstance(v, str): text_blobs.append(v)
    return {
        "record_id": rec.get("id"),
        "doi": rec.get("doi") or md.get("doi"),
        "conceptdoi": rec.get("conceptdoi") or rec.get("concept_doi") or md.get("conceptdoi"),
        "conceptrecid": rec.get("conceptrecid"),
        "created": rec.get("created"),
        "updated": rec.get("updated"),
        "links_self": (rec.get("links") or {}).get("self"),
        "links_html": (rec.get("links") or {}).get("html"),
        "files": rec.get("files"),
        "title": title,
        "description": description,
        "language": md.get("language"),
        "publication_date": md.get("publication_date") or md.get("publicationdate"),
        "creators": creators,
        "contributors": contributors,
        "keywords": keywords,
        "communities": md.get("communities") or [],
        "grants": md.get("grants") or [],
        "related_identifiers": md.get("related_identifiers") or md.get("relatedidentifiers") or [],
        "locations": locations,
        "dates": md.get("dates") or [],
        "has_cz_text_hit": any(text_has_cz(t) for t in text_blobs),
    }

def explode_creators(record_row: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = []
    for c in record_row.get("creators") or []:
        name = c.get("name") or c.get("person_or_org", {}).get("name")
        affiliation = (c.get("affiliation")
            or (c.get("affiliations", [{}])[0].get("name")
                if isinstance(c.get("affiliations"), list) and c.get("affiliations")
                else c.get("affiliations")))
        orcid = (c.get("orcid")
            or next((i.get("identifier") for i in c.get("person_or_org", {}).get("identifiers", [])
                     if i.get("scheme") == "orcid"), None))
        out.append({
            "record_id": record_row.get("record_id"),
            "doi": record_row.get("doi"),
            "author_name": name,
            "affiliation": affiliation,
            "orcid": orcid,
            "has_cz_affil": text_has_cz(affiliation),
        })
    return out

# ---------- affiliation matching ----------
def load_affiliations_list(path: Optional[str]) -> List[str]:
    if not path: return []
    p = pathlib.Path(path)
    if not p.exists():
        logging.warning("Affiliations file not found: %s", path); return []
    lines = [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
    normed = sorted(set(norm_text(x) for x in lines))
    return [x for x in normed if x]

def find_affil_matches(creators_df: pd.DataFrame, affil_list: List[str]) -> pd.DataFrame:
    if creators_df.empty or not affil_list:
        return pd.DataFrame(columns=["record_id","doi","author_name","affiliation","matched_institution"])
    tmp = creators_df[["record_id","doi","author_name","affiliation"]].copy()
    tmp["affil_norm"] = tmp["affiliation"].fillna("").map(norm_text)
    rows = []
    for _, r in tmp.iterrows():
        for inst in affil_list:
            if inst and inst in r["affil_norm"]:
                rows.append({
                    "record_id": r["record_id"], "doi": r["doi"],
                    "author_name": r["author_name"], "affiliation": r["affiliation"],
                    "matched_institution": inst,
                })
    return pd.DataFrame(rows)

# ---------- counting + planning ----------
def get_total(sess: requests.Session, base_params: dict) -> int:
    p = dict(base_params); p["size"] = 1; p["page"] = 1
    r = sess.get(ZENODO_API, params=p, timeout=60)
    if r.status_code == 429:
        wait = int(r.headers.get("Retry-After", "5")); logging.info("429 on total; sleep %ss", wait); time.sleep(wait); return get_total(sess, base_params)
    if r.status_code >= 500: logging.info("5xx on total; retry shortly"); time.sleep(3); return get_total(sess, base_params)
    r.raise_for_status()
    return r.json().get("hits", {}).get("total", 0)

def enumerate_windows(sess: requests.Session, base_params: dict, field: str, start: date, end: date, hard_cap: int=9500) -> List[Dict[str, Any]]:
    to_dt = lambda d: datetime(d.year, d.month, d.day)
    queue: Deque[Tuple[datetime, datetime]] = deque([(to_dt(start), to_dt(end))])
    result: List[Dict[str, Any]] = []
    pbar = tqdm(total=1, desc=f"Counting windows ({field})", unit="win")
    processed = 0
    while queue:
        ws, we = queue.popleft()
        q_range = f'{field}:[{iso_date(ws)} TO {iso_date(we)}]'
        params = dict(base_params, q=q_range)
        total = get_total(sess, params)
        processed += 1; pbar.total = processed + len(queue); pbar.update(1)
        pbar.set_postfix_str(f"win={iso_date(ws)}..{iso_date(we)} total={total}")
        if total == 0: continue
        if total <= hard_cap or (we - ws).days <= 1:
            result.append({"start": iso_date(ws), "end": iso_date(we), "total": int(total)})
        else:
            mid = ws + (we - ws) / 2
            queue.append((ws, mid)); queue.append((mid + timedelta(days=1), we))
    pbar.close(); logging.info("Calculated %d windows (<= %d hits each).", len(result), hard_cap)
    return result

# ---------- harvesting with auto-split on 5xx ----------
def fetch_page(sess: requests.Session, params: dict, timeout_s: int=60):
    return sess.get(ZENODO_API, params=params, timeout=timeout_s)

def harvest_with_queue(out_dir: pathlib.Path, sess: requests.Session, base: dict, initial_windows: List[Dict[str, Any]],
                       throttle_per_minute: int, initial_size: int, field: str, hard_cap: int):
    state_dir = out_dir / "state"; safe_mkdir(state_dir)
    jsonl_path = out_dir / "raw" / "records.jsonl"; safe_mkdir(jsonl_path.parent)
    if not jsonl_path.exists(): jsonl_path.write_text("", encoding="utf-8")

    queue_path = state_dir / "queue.json"
    state_path = state_dir / "state.json"

    # init or resume queue
    if queue_path.exists():
        windows = deque(load_json(queue_path, []))
        if not windows:
            windows = deque({"start": w["start"], "end": w["end"], "page": 1, "size": initial_size, "total": w.get("total", 0)} for w in initial_windows)
    else:
        windows = deque({"start": w["start"], "end": w["end"], "page": 1, "size": initial_size, "total": w.get("total", 0)} for w in initial_windows)

    time_per_req = 60.0 / max(1, throttle_per_minute)
    pbar = tqdm(total=len(windows), desc="Harvesting (windows)", unit="win")

    while windows:
        w = windows[0]  # current
        start, end = w["start"], w["end"]; page = int(w.get("page", 1)); size = int(w.get("size", initial_size))
        total = int(w.get("total", 0))
        if total <= 0:
            # získej přesný total pro lepší odhad max_pages
            total = get_total(sess, {"type": "dataset", "all_versions": 0, "sort": "mostrecent",
                                     "q": f'{field}:[{start} TO {end}]'})
            w["total"] = total
        max_pages = max(1, math.ceil(total / size) + 2)

        logging.info("Window %s → %s (est.total=%s, page=%d, size=%d, max_pages=%d)", start, end, total, page, size, max_pages)
        page_pbar = tqdm(total=max_pages, initial=page-1, desc=f"  Pages {start}..{end}", unit="page", leave=False)

        consecutive_5xx = 0
        last_progress_t = time.time()

        while page <= max_pages:
            t0 = time.time()
            params = {"type":"dataset","all_versions":0,"sort":"mostrecent","size":size,"page":page, "q": f'{field}:[{start} TO {end}]'}
            try:
                r = fetch_page(sess, params)
            except requests.RequestException as e:
                logging.warning("Request exception p%d: %s (sleep 5s)", page, e); time.sleep(5); continue

            rl_rem = r.headers.get("X-RateLimit-Remaining"); rl_lim = r.headers.get("X-RateLimit-Limit"); rl_res = r.headers.get("X-RateLimit-Reset")

            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", "5"))
                logging.info("429 p%d. Retry-After=%ss (rate %s/%s reset %s)", page, wait, rl_rem, rl_lim, rl_res)
                time.sleep(wait); continue
            if r.status_code >= 500:
                consecutive_5xx += 1
                logging.info("5xx p%d (try %d). Retry 3s", page, consecutive_5xx); time.sleep(3)
                # auto-split trigger: příliš mnoho 5xx nebo dlouho bez progresu
                if consecutive_5xx >= 8 or (time.time() - last_progress_t) > 300:
                    # pokud je okno alespoň 2denní → rozděl
                    d0 = datetime.fromisoformat(start); d1 = datetime.fromisoformat(end)
                    if (d1 - d0).days >= 1:
                        mid = d0 + (d1 - d0) / 2
                        left = {"start": start, "end": mid.strftime("%Y-%m-%d"), "page": 1, "size": max(50, size//2), "total": 0}
                        right = {"start": (mid + timedelta(days=1)).strftime("%Y-%m-%d"), "end": end, "page": 1, "size": max(50, size//2), "total": 0}
                        logging.warning("Auto-splitting window %s..%s due to persistent 5xx → [%s..%s] + [%s..%s]",
                                        start, end, left["start"], left["end"], right["start"], right["end"])
                        # replace current by two smaller
                        windows.popleft()
                        windows.appendleft(right); windows.appendleft(left)
                        save_json(queue_path, list(windows))
                        page_pbar.close()
                        break  # break page loop → continue with left
                    else:
                        # Jednodenní okno: uber size a zkus dál
                        if size > 50:
                            new_size = max(50, size // 2)
                            logging.warning("Stubborn single-day window %s..%s: reducing size %d→%d", start, end, size, new_size)
                            w["size"] = new_size; save_json(queue_path, list(windows))
                        consecutive_5xx = 0
                continue
            if r.status_code == 400:
                logging.warning("400 p%d. Stopping this window early.", page); break

            r.raise_for_status()
            items = r.json().get("hits", {}).get("hits", [])
            n = len(items)
            logging.info("win %s..%s page %d: %d items (rate %s/%s reset %s)", start, end, page, n, rl_rem, rl_lim, rl_res)

            if n == 0: break  # okno vyčerpáno

            write_jsonl(jsonl_path, items)
            page += 1; w["page"] = page; consecutive_5xx = 0; last_progress_t = time.time()
            save_json(queue_path, list(windows))
            page_pbar.update(1)

            # throttle
            dt = time.time() - t0
            if dt < time_per_req: time.sleep(time_per_req - dt)

        else:
            # while not broken: finished pages
            pass

        # pokud jsme breakli kvůli splitu, neodstraňuj okno (už je nahrazeno menšími)
        if windows and windows[0] is w:
            windows.popleft()  # window finished
            save_json(queue_path, list(windows))
            pbar.update(1)

        page_pbar.close()

    pbar.close()
    logging.info("Harvesting finished. RAW at %s", jsonl_path)

# ---------- post-processing ----------
def load_raw_to_parquet(out_dir: pathlib.Path):
    raw_jsonl = out_dir / "raw" / "records.jsonl"; parquet_dir = out_dir / "parquet"; safe_mkdir(parquet_dir)
    if not raw_jsonl.exists() or raw_jsonl.stat().st_size == 0:
        logging.warning("RAW missing/empty: %s", raw_jsonl); return
    rec_rows, creators_rows = [], []
    with raw_jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            rec = json.loads(line); flat = flatten_record(rec)
            rec_rows.append(flat); creators_rows.extend(explode_creators(flat))
    if not rec_rows: logging.warning("No records parsed."); return
    df_rec = pd.DataFrame(rec_rows)
    df_cre = pd.DataFrame(creators_rows if creators_rows else [],
                          columns=["record_id","doi","author_name","affiliation","orcid","has_cz_affil"])
    if not df_cre.empty:
        has_cz = df_cre.groupby("record_id")["has_cz_affil"].any().rename("has_cz_affil_any_author")
        df_rec = df_rec.merge(has_cz, on="record_id", how="left")
    else:
        df_rec["has_cz_affil_any_author"] = False
    df_rec["has_cz_affil_any_author"] = df_rec["has_cz_affil_any_author"].fillna(False)
    df_rec.to_parquet(parquet_dir / "records.parquet", index=False)
    df_cre.to_parquet(parquet_dir / "creators.parquet", index=False)
    logging.info("Parquet exported to %s", parquet_dir)

def mark_affiliation_matches(out_dir: pathlib.Path, affiliations_file: Optional[str]):
    if not affiliations_file: return
    parquet_dir = out_dir / "parquet"; matches_dir = out_dir / "matches"; safe_mkdir(matches_dir)
    rec_path = parquet_dir / "records.parquet"; cre_path = parquet_dir / "creators.parquet"
    if not rec_path.exists() or not cre_path.exists(): logging.warning("Parquets missing; skip matching."); return
    df_rec = pd.read_parquet(rec_path); df_cre = pd.read_parquet(cre_path)
    affil_list = load_affiliations_list(affiliations_file)
    df_hits = find_affil_matches(df_cre, affil_list)
    if not df_hits.empty:
        df_hits.to_parquet(matches_dir / "affil_hits.parquet", index=False)
        has_match = df_hits.groupby("record_id").size().rename("affil_hit_count")
        df_rec = df_rec.merge(has_match, on="record_id", how="left")
    else:
        df_rec["affil_hit_count"] = 0
    df_rec["affil_hit_count"] = df_rec["affil_hit_count"].fillna(0).astype("int64")
    df_rec["has_affil_match"] = df_rec["affil_hit_count"] > 0
    df_rec.to_parquet(rec_path, index=False)
    logging.info("Affiliation matches done.")

def export_duckdb(out_dir: pathlib.Path):
    parquet_dir = out_dir / "parquet"; db_path = out_dir / "zenodo.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("PRAGMA threads=4;")
    con.execute("CREATE OR REPLACE TABLE records AS SELECT * FROM read_parquet(?);", [str(parquet_dir / "records.parquet")])
    con.execute("CREATE OR REPLACE TABLE creators AS SELECT * FROM read_parquet(?);", [str(parquet_dir / "creators.parquet")])
    con.execute("""
        CREATE OR REPLACE TABLE records_cz AS
        SELECT * FROM records
        WHERE has_cz_text_hit OR has_cz_affil_any_author OR COALESCE(has_affil_match, FALSE);
    """)
    con.close(); logging.info("DuckDB saved at %s", db_path)

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Zenodo datasets harvest (resumable, auto-splitting)")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--size", type=int, default=200)
    ap.add_argument("--throttle-per-minute", type=int, default=55)
    ap.add_argument("--window-field", default="updated", choices=["created","updated","publication_date"])
    ap.add_argument("--date-from", default=None)
    ap.add_argument("--date-to", default=None)
    ap.add_argument("--hard-cap", type=int, default=9500)
    ap.add_argument("--affiliations-file", default=None)
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s")

    out_dir = pathlib.Path(args.out_dir); safe_mkdir(out_dir); safe_mkdir(out_dir / "raw"); safe_mkdir(out_dir / "state")
    sess = build_session()

    start = dateparser.parse(args.date_from).date() if args.date_from else date(1990,1,1)
    end = dateparser.parse(args.date_to).date() if args.date_to else datetime.now(timezone.utc).date()

    base_count = {"type":"dataset","all_versions":0,"sort":"mostrecent"}
    preferred = [args.window_field] + [f for f in ("updated","publication_date","created") if f != args.window_field]
    windows = []
    field = preferred[0]
    for fld in preferred:
        try:
            windows = enumerate_windows(sess, base_count, fld, start, end, args.hard_cap)
            field = fld; break
        except requests.HTTPError as e:
            logging.warning("Counting failed for %s (%s), trying next…", fld, e)
    if not windows: raise SystemExit("Failed to compute windows.")

    harvest_with_queue(out_dir, sess, base_count, windows,
                       args.throttle_per_minute, args.size, field, args.hard_cap)

    load_raw_to_parquet(out_dir)
    mark_affiliation_matches(out_dir, args.affiliations_file)
    export_duckdb(out_dir)
    logging.info("All done. Outputs in: %s", out_dir)

if __name__ == "__main__":
    main()

