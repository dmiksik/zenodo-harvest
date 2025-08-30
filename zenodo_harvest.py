#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zenodo_harvest.py — robustní sklizeň Zenodo "dataset" záznamů s automatickým dělením oken.

Hlavní vlastnosti
- Sklizeň "dataset" (all_versions=false) se stabilním stránkováním a retry/backoff.
- Dělení časového okna (podle --window-field) binárně na menší řezy, dokud v každém
  okně není <= --hard-cap výsledků (bezpečně pod limitem 10k/požadavek).
- Dotazy používají INKLUZIVNÍ rozsahy s horní mezí posunutou o 1 ms zpět, aby se okna
  nepřekrývala; formát pole timestampů s uvozovkami: field:["start" TO "end"].
- Horní hranice sklizně je při startu "zmrazena" (snapshot), aby „neujížděla“ během běhu.
- Výstup: RAW JSONL (append/overwrite). Jednoduchý .state JSON pro navázání.

Příklad:
    python zenodo_harvest.py --out-dir ./zenodo_dump \
      --window-field created --date-from 1990-01-01 \
      --size 250 --throttle-per-minute 55 --hard-cap 9000

Poznámka:
- Pro maximální konzistenci doporučuji primární sklizeň dělit podle `created`
  a případně dodat sekundární krátký průchod pro dohánění editací přes `updated`
  jen za poslední dny/týdny.
"""

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple

import requests
from requests.exceptions import RequestException

Z_API = "https://zenodo.org/api/records"
ONE_MS = timedelta(microseconds=1000)

# -------------------------
# Pomocné funkce
# -------------------------

def iso_z(dt: datetime) -> str:
    """ISO8601 UTC s 'Z'."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_inclusive_range(field: str, start_iso: str, end_iso: str) -> str:
    """
    Oboje inkluzivní hranice s uvozovkami:
        field:["start_iso" TO "end_iso"]
    Horní mez okna posouváme při použití o 1 ms zpět, aby se okna nepřekrývala.
    """
    return f'{field}:["{start_iso}" TO "{end_iso}"]'


def extract_total(data: dict) -> int:
    """Bezpečně vytáhne total (int nebo {'value': int})."""
    hits = data.get("hits", {})
    total = hits.get("total", 0)
    if isinstance(total, dict):
        total = total.get("value", 0)
    try:
        return int(total)
    except Exception:
        return 0


def request_with_retries(session, url, params, timeout, max_retries=6):
    """
    GET s retry/backoff. Ošetří 429 (Retry-After) a 5xx.
    """
    attempt = 0
    while True:
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                wait = float(ra) if ra else max(5.0, 2.0 ** attempt)
                time.sleep(wait); attempt += 1
                if attempt > max_retries: r.raise_for_status()
                continue
            if 500 <= r.status_code < 600:
                wait = max(3.0, 2.0 ** attempt)
                time.sleep(wait); attempt += 1
                if attempt > max_retries: r.raise_for_status()
                continue
            r.raise_for_status()
            return r
        except RequestException:
            wait = max(3.0, 2.0 ** attempt)
            time.sleep(wait); attempt += 1
            if attempt > max_retries:
                raise


def get_total(session: requests.Session, q: str, all_versions: bool = False) -> int:
    """
    Vrátí jen počet záznamů (Zenodo na tomhle endpointu odmítá size=0 → použijeme size=1).
    """
    params = {
        "q": q,
        "all_versions": "true" if all_versions else "false",
        "size": 1,
    }
    r = request_with_retries(session, Z_API, params, timeout=60)
    return extract_total(r.json())


# -------------------------
# Stav / okna
# -------------------------

@dataclass
class Window:
    start_iso: str          # ISOUTC včetně Z (levá inkluzivní)
    end_excl_iso: str       # ISOUTC (pravá EXKLUZIVNÍ, do dotazu půjde end-1ms)
    est_total: Optional[int] = None
    done: bool = False

    def as_range_query(self, field: str) -> str:
        # Dotaz používá horní mez posunutou o 1 ms zpět
        s_dt = datetime.strptime(self.start_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        e_dt_excl = datetime.strptime(self.end_excl_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        rng = build_inclusive_range(field, iso_z(s_dt), iso_z(e_dt_excl - ONE_MS))
        return rng

    def to_tuple(self) -> Tuple[datetime, datetime]:
        s = datetime.strptime(self.start_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        e = datetime.strptime(self.end_excl_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return s, e


@dataclass
class HarvestState:
    snapshot_end_iso: str
    windows: List[Window]

    @staticmethod
    def load(path: str) -> Optional["HarvestState"]:
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        windows = [Window(**w) for w in d.get("windows", [])]
        return HarvestState(snapshot_end_iso=d["snapshot_end_iso"], windows=windows)

    def save(self, path: str) -> None:
        d = {
            "snapshot_end_iso": self.snapshot_end_iso,
            "windows": [asdict(w) for w in self.windows],
        }
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)


# -------------------------
# Dělení oken podle počtu
# -------------------------

def split_windows_by_count(session: requests.Session,
                           base_query_prefix: str,
                           field: str,
                           start_dt: datetime,
                           end_dt_excl: datetime,
                           hard_cap: int,
                           throttle_s: float) -> List[Window]:
    """
    Rozdělí [start, end) na menší okna tak, aby v každém okně bylo <= hard_cap záznamů.
    Vrací seřazený seznam Window (start_iso, end_excl_iso, est_total).
    """
    stack: List[Tuple[datetime, datetime]] = [(start_dt, end_dt_excl)]
    out: List[Window] = []
    while stack:
        s, e_excl = stack.pop()
        rng_q = build_inclusive_range(field, iso_z(s), iso_z(e_excl - ONE_MS))
        q = f"{base_query_prefix} AND {rng_q}"
        total = get_total(session, q)

        if total <= hard_cap:
            out.append(Window(iso_z(s), iso_z(e_excl), est_total=total, done=False))
            time.sleep(throttle_s)
            continue

        span = (e_excl - s).total_seconds()
        if span <= 1:
            out.append(Window(iso_z(s), iso_z(e_excl), est_total=total, done=False))
            time.sleep(throttle_s)
            continue

        half = max(1, int(span // 2))
        mid = s + timedelta(seconds=half)
        if mid <= s:
            mid = s + timedelta(seconds=1)
        if mid >= e_excl:
            mid = e_excl - timedelta(seconds=1)

        # pravé, pak levé (LIFO → levé se zpracuje dřív)
        stack.append((mid, e_excl))
        stack.append((s, mid))
        time.sleep(throttle_s)

    out.sort(key=lambda w: w.start_iso)
    return out


# -------------------------
# Stahování jednoho okna
# -------------------------

def fetch_fixed_window(session: requests.Session,
                       base_query_prefix: str,
                       field: str,
                       window: Window,
                       size: int,
                       throttle_s: float,
                       out_fh) -> int:
    """
    Stáhne jedno okno (range podle window) po stránkách. Adaptivně zmenší size při 5xx.
    Vrací počet zapsaných záznamů.
    """
    rng_q = window.as_range_query(field)
    q = f"{base_query_prefix} AND {rng_q}"

    page = 1
    current_size = size
    written = 0

    while True:
        params = {
            "q": q,
            "all_versions": "false",
            "size": current_size,
            "page": page,
            "sort": "mostrecent",
        }
        try:
            r = request_with_retries(session, Z_API, params, timeout=120)
            data = r.json()
            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                break
            for rec in hits:
                out_fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                written += 1
            page += 1
            time.sleep(throttle_s)
            # postupné „vracení“ na původní size, když to drží
            if current_size < size:
                current_size = min(size, current_size * 2)
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status and 500 <= status < 600:
                current_size = max(50, current_size // 2)
                continue
            raise

    return written


# -------------------------
# Hlavní běh
# -------------------------

def parse_args():
    ap = argparse.ArgumentParser(description="Harvest Zenodo datasets with window splitting.")
    ap.add_argument("--out-dir", required=True, help="Výstupní adresář (bude vytvořen)")
    ap.add_argument("--size", type=int, default=250, help="Velikost stránky (<=1000)")
    ap.add_argument("--throttle-per-minute", type=int, default=55, help="Požadavků za minutu")
    ap.add_argument("--window-field", choices=["created", "updated", "publication_date"], default="created",
                    help="Pole pro dělení časových oken")
    ap.add_argument("--date-from", required=True, help="Počáteční datum (YYYY-MM-DD, UTC)")
    ap.add_argument("--date-to", default=None, help="Koncové datum (YYYY-MM-DD, UTC), default=NOW (snapshot)")
    ap.add_argument("--hard-cap", type=int, default=9000, help="Limit záznamů na okno před dělením")
    ap.add_argument("--append", action="store_true", help="Append do RAW JSONL (jinak se přepíše)")
    ap.add_argument("--state-file", default=None, help="Cesta k .state JSON pro navázání (default: <out-dir>/state.json)")
    return ap.parse_args()


def main():
    args = parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    raw_jsonl_path = os.path.join(args.out_dir, "records.jsonl")
    state_path = args.state_file or os.path.join(args.out_dir, "state.json")

    throttle_s = max(0.0, 60.0 / float(args.throttle_per_minute))

    # Snapshot horní hranice
    if args.date_to:
        snapshot_end = datetime.strptime(args.date_to, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    else:
        snapshot_end = datetime.utcnow().replace(tzinfo=timezone.utc)

    date_from_dt = datetime.strptime(args.date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_dt = date_from_dt
    end_dt_excl = snapshot_end

    # Prefix dotazu: mirror GUI
    base_query_prefix = "(resource_type.type:dataset)"
    field = args.window_field

    # Načti/počítej okna
    state = HarvestState.load(state_path)
    if state is None:
        # Vypočti okna dle skutečných počtů
        with requests.Session() as sess:
            print(f"[1/3] Vypočítávám okna pro {field} v intervalu [{iso_z(start_dt)} .. {iso_z(end_dt_excl)}) "
                  f"při hard_cap={args.hard_cap} …")
            windows = split_windows_by_count(sess, base_query_prefix, field,
                                             start_dt, end_dt_excl, args.hard_cap, throttle_s)
        state = HarvestState(snapshot_end_iso=iso_z(snapshot_end), windows=windows)
        state.save(state_path)
        print(f"     Oken: {len(windows)}; odhad celkem: {sum(w.est_total or 0 for w in windows)}")
    else:
        print(f"[1/3] Načten stav ze {state_path}; zbývá {sum(1 for w in state.windows if not w.done)} oken.")

    # Otevři RAW
    mode = "a" if args.append else "w"
    with open(raw_jsonl_path, mode, encoding="utf-8") as out_fh, requests.Session() as sess:
        print(f"[2/3] Sklízím okna do {raw_jsonl_path} (mode={mode}) …")
        for i, w in enumerate(state.windows):
            if w.done:
                continue
            s_dt, e_dt = w.to_tuple()
            print(f"     Okno {i+1}/{len(state.windows)}: {w.start_iso} .. {w.end_excl_iso}"
                  + (f"  (est={w.est_total})" if w.est_total is not None else ""))
            try:
                wrote = fetch_fixed_window(sess, base_query_prefix, field, w, args.size, throttle_s, out_fh)
                print(f"       → zapsáno {wrote} záznamů")
                w.done = True
                state.save(state_path)
            except Exception as e:
                print(f"       ! chyba: {e}", file=sys.stderr)
                # ulož průběžně a skonči s chybou, ať lze navázat
                state.save(state_path)
                raise

    print(f"[3/3] Hotovo. RAW: {raw_jsonl_path}")
    print(f"       Stav uložen: {state_path} (všechna okna {'hotová' if all(w.done for w in state.windows) else 'NEhotová'})")


if __name__ == "__main__":
    main()

