#!/usr/bin/env python3
"""
scrape_bensinpriser.py

- Strict caps enforcement with pending counts tracked per (fuel, date) —
  this is the only way the cap can be enforced consistently, since the
  existing-row counts loaded from the CSV are also per (fuel, date).
  (An earlier version tracked pending counts per-fuel only, which meant
  that scraping two different dates for the same fuel in one run let the
  cap silently spill counts from one date onto another. Fixed here.)
- Rejects prices outside MIN_PRICE..MAX_PRICE.
- Rejects rows whose Date is after ScrapeDate (impossible / bad parse).
- Appends only truly new rows (Station+Price+Date+Fuel normalized).
- Never deletes/replaces existing CSV rows; backs up the CSV before writing.
- Isolates per-fuel network/parse failures so one bad page doesn't kill the run.
- Accurate summary printed (and logged) at the end.
"""

import logging
import re
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------- Config ----------
HEADERS = {"User-Agent": "Mozilla/5.0"}
CSV_FILE = Path("bensinpriser_prices.csv")
REQUEST_TIMEOUT = 15  # seconds
LOG_FILE = Path("scrape_bensinpriser.log")

FUEL_URLS = {
    "95 (E10)": "https://bensinpriser.nu/stationer/95/alla/alla",
    "98": "https://bensinpriser.nu/stationer/98/alla/alla",
    "Diesel": "https://bensinpriser.nu/stationer/diesel/alla/alla",
    "Etanol": "https://bensinpriser.nu/stationer/etanol/alla/alla",
}

MIN_PRICE = {"95 (E10)": 14.0, "98": 14.0, "Diesel": 14.0, "Etanol": 11.0}
MAX_PRICE = {"95 (E10)": 27.0, "98": 30.0, "Diesel": 35.0, "Etanol": 35.0}
ROW_LIMITS = {"95 (E10)": 6, "98": 3, "Diesel": 6, "Etanol": 3}
COLS_ORDER = ["Station", "Price", "Date", "Fuel", "ScrapeDate", "Source"]

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------- HTTP session with retries ----------
def make_session():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.headers.update(HEADERS)
    return session


# ---------- Helpers ----------
def parse_number(s):
    """Parse localized price strings like '14,71kr' -> float or None."""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    cleaned = re.sub(r"[^\d,.\-]", "", s).replace(" ", "")
    if "," in cleaned and "." not in cleaned:
        cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def normalize_date_str(raw, today=None):
    """Normalize date tokens to ISO YYYY-MM-DD. Accepts 'Idag','Igår','15/9','2025-09-15'.

    `today` can be injected for testability; defaults to date.today().
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    low = s.lower()
    today = today or date.today()

    if low.startswith("idag"):
        return today.isoformat()
    if low.startswith("igår") or low.startswith("igar") or low.startswith("ig"):
        return (today - timedelta(days=1)).isoformat()

    # ISO already?
    m_iso = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s)
    if m_iso:
        y, mo, d = m_iso.groups()
        try:
            return date(int(y), int(mo), int(d)).isoformat()
        except ValueError:
            return None

    # dd/mm — assume current year unless that would put the date in the
    # future, in which case assume the previous year.
    m = re.match(r"^(\d{1,2})/(\d{1,2})$", s)
    if m:
        day, month = int(m.group(1)), int(m.group(2))
        yr = today.year
        try:
            candidate = date(yr, month, day)
        except ValueError:
            return None
        if candidate > today:
            yr -= 1
            try:
                candidate = date(yr, month, day)
            except ValueError:
                return None
        return candidate.isoformat()

    return None


def normalize_fuel(f):
    """Canonicalize fuel names to keys in FUEL_URLS / ROW_LIMITS."""
    if f is None:
        return None
    fs = str(f).strip()
    for key in FUEL_URLS.keys():
        if fs.lower() == key.lower():
            return key
    low = fs.lower()
    if "95" in low:
        return "95 (E10)"
    if "98" in low:
        return "98"
    if "diesel" in low:
        return "Diesel"
    if "etanol" in low:
        return "Etanol"
    return fs


def make_key_norm(station, price, date_iso, fuel):
    """Normalized uniqueness key: station_lower_trim, price rounded to 3 decimals, date_iso, fuel_lower."""
    st = "" if station is None else str(station).strip().lower()
    try:
        p = float(price) if price is not None and str(price).strip() != "" else None
    except (ValueError, TypeError):
        p = None
    pstr = f"{p:.3f}" if p is not None else ""
    d = "" if date_iso is None else str(date_iso).strip()
    f = "" if fuel is None else str(fuel).strip().lower()
    return (st, pstr, d, f)


def is_valid_date_pair(date_iso, scrape_date_iso):
    """Date logic must make sense: the price's Date can never be after the
    day it was scraped (ScrapeDate). Anything else indicates a bad parse
    (e.g. misread dd/mm, clock skew) and the row should be dropped."""
    if date_iso is None or scrape_date_iso is None:
        return False
    try:
        d = date.fromisoformat(date_iso)
        sd = date.fromisoformat(scrape_date_iso)
    except ValueError:
        return False
    return d <= sd


# ---------- Scrape a single URL ----------
def scrape_one_url(session, url, fuel_key, today_iso):
    """Return list of normalized row dicts scraped from one fuel page.
    Raises on network failure so the caller can isolate/skip this fuel."""
    resp = session.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    out = []
    fk = normalize_fuel(fuel_key)
    skipped_unparseable = 0

    rows = soup.find_all("tr", class_="table-row")
    if not rows:
        log.warning("No rows found for %s at %s — page markup may have changed.", fk, url)

    for tr in rows:
        tds = tr.find_all("td")
        if not tds:
            continue
        station = tds[0].get_text(strip=True)
        s_low = station.lower()
        if (
            "costco" in s_low
            or "medlemsskap krävs" in s_low
            or "medlemskap krævs" in s_low
            or "medlemskap krävs" in s_low
            or s_low.startswith("tips")
        ):
            continue

        price_val = None
        date_iso = None
        if len(tds) > 1:
            price_td = tds[1]
            small = price_td.find("small")
            if small:
                raw_date = small.get_text(strip=True)
                date_iso = normalize_date_str(raw_date)
                small.extract()
            price_txt = price_td.get_text(strip=True)
            price_val = parse_number(price_txt)

        if price_val is None or date_iso is None:
            skipped_unparseable += 1
            continue

        # Logical sanity: a price cannot be dated after today (the scrape date).
        if not is_valid_date_pair(date_iso, today_iso):
            log.warning(
                "Dropping row with implausible date: station=%r fuel=%s date=%s scrape_date=%s",
                station, fk, date_iso, today_iso,
            )
            continue

        if price_val < MIN_PRICE.get(fk, 0.0) or price_val > MAX_PRICE.get(fk, float("inf")):
            continue

        out.append({
            "Station": station,
            "Price": price_val,
            "Date": date_iso,
            "Fuel": fk,
            "ScrapeDate": today_iso,
            "Source": "bensinpriser.nu",
        })

    if skipped_unparseable:
        log.info("%s: skipped %d row(s) with unparseable price/date.", fk, skipped_unparseable)

    return out


def backup_csv(path: Path):
    if not path.exists():
        return None
    backup_path = path.with_suffix(path.suffix + ".bak")
    shutil.copy2(path, backup_path)
    log.info("Backed up existing CSV to %s", backup_path)
    return backup_path


# ---------- Main ----------
def main():
    today_iso = date.today().isoformat()

    # Load existing rows and build normalized key set + per-(fuel,date) counts.
    existing_keys = set()
    counts_per_fd = {}  # (fuel_norm, date_iso) -> count of existing rows
    df_old = pd.DataFrame(columns=COLS_ORDER)
    if CSV_FILE.exists():
        df_old = pd.read_csv(CSV_FILE, dtype=object)
        for _, r in df_old.iterrows():
            fuel_norm = normalize_fuel(r.get("Fuel"))
            date_iso = normalize_date_str(r.get("Date")) or r.get("Date")
            price_v = parse_number(r.get("Price"))
            key = make_key_norm(r.get("Station"), price_v, date_iso, fuel_norm)
            existing_keys.add(key)
            counts_per_fd[(fuel_norm, date_iso)] = counts_per_fd.get((fuel_norm, date_iso), 0) + 1

    # Collect candidates across all fuels, isolating failures per fuel.
    session = make_session()
    candidates = []
    failed_fuels = []
    for fuel_key, url in FUEL_URLS.items():
        try:
            scraped = scrape_one_url(session, url, fuel_key, today_iso)
            log.info("%s: scraped %d candidate row(s).", fuel_key, len(scraped))
            candidates.extend(scraped)
        except requests.RequestException as e:
            log.error("Failed to fetch %s (%s): %s", fuel_key, url, e)
            failed_fuels.append(fuel_key)
        except Exception as e:  # parsing errors etc. — don't let one page kill the run
            log.error("Failed to parse %s (%s): %s", fuel_key, url, e)
            failed_fuels.append(fuel_key)

    # STRICT CAP ENFORCEMENT, now consistently per (fuel, date) for both
    # existing rows and rows accepted within this run.
    pending_counts_by_fd = {}   # (fuel_norm, date_iso) -> int
    pending_keys = set()
    to_append = []
    skipped_dup = 0
    skipped_cap = 0
    skipped_price = 0
    added = 0

    for r in candidates:
        fuel_norm = normalize_fuel(r["Fuel"])
        date_iso = r["Date"]  # already normalized in scrape_one_url
        price_val = float(r["Price"])
        key = make_key_norm(r["Station"], price_val, date_iso, fuel_norm)

        # Duplicate check (against existing and pending)
        if key in existing_keys or key in pending_keys:
            skipped_dup += 1
            continue

        # Price bounds sanity (defense in depth)
        if price_val < MIN_PRICE.get(fuel_norm, 0.0) or price_val > MAX_PRICE.get(fuel_norm, float("inf")):
            skipped_price += 1
            continue

        fd = (fuel_norm, date_iso)
        existing_count = counts_per_fd.get(fd, 0)
        pending_count = pending_counts_by_fd.get(fd, 0)

        cap = ROW_LIMITS.get(fuel_norm)
        if cap is not None and (existing_count + pending_count) >= cap:
            skipped_cap += 1
            continue

        pending_keys.add(key)
        pending_counts_by_fd[fd] = pending_count + 1

        to_append.append({
            "Station": r["Station"],
            "Price": price_val,
            "Date": date_iso,
            "Fuel": fuel_norm,
            "ScrapeDate": r.get("ScrapeDate"),
            "Source": r.get("Source", "bensinpriser.nu"),
        })
        added += 1

    # Append to CSV if to_append non-empty (back up first, never overwrite blindly).
    if to_append:
        backup_csv(CSV_FILE)
        df_append = pd.DataFrame(to_append, columns=COLS_ORDER)
        df_append["Price"] = pd.to_numeric(df_append["Price"], errors="coerce")
        if CSV_FILE.exists():
            df_combined = pd.concat([df_old, df_append], ignore_index=True, sort=False)
        else:
            df_combined = df_append.copy()
        for c in COLS_ORDER:
            if c not in df_combined.columns:
                df_combined[c] = None
        df_combined = df_combined[COLS_ORDER]
        df_combined.to_csv(CSV_FILE, index=False)
    else:
        df_combined = df_old.copy()

    # Accurate summary
    summary_lines = [
        "Summary:",
        f"  Added: {added}",
        f"  Skipped (duplicate exact match): {skipped_dup}",
        f"  Skipped (cap reached): {skipped_cap}",
        f"  Skipped (price out of bounds): {skipped_price}",
        f"  Fuels that failed to fetch/parse: {failed_fuels if failed_fuels else 'none'}",
        f"  Total rows after run: {len(df_combined)}",
    ]
    for line in summary_lines:
        log.info(line)

    if added:
        log.info("Added rows:")
        for r in to_append:
            log.info(" - %s | %s kr | %s | %s", r["Station"], r["Price"], r["Date"], r["Fuel"])

    # Non-zero exit code if every single fuel failed, so schedulers/monitors notice.
    if failed_fuels and len(failed_fuels) == len(FUEL_URLS):
        log.error("All fuel pages failed — treating this run as a failure.")
        sys.exit(1)


if __name__ == "__main__":
    main()
