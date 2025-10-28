#!/usr/bin/env python3
"""
scrape_bensinpriser.py

- Strict caps enforcement with pending counts tracked per-fuel (not per fuel+date)
  as requested.
- Rejects prices outside MIN_PRICE..MAX_PRICE.
- Appends only truly new rows (Station+Price+Date+Fuel normalized).
- Never deletes/replaces existing CSV rows.
- Accurate summary printed at the end.
"""

import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date, timedelta
from pathlib import Path

# ---------- Config ----------
HEADERS = {"User-Agent": "Mozilla/5.0"}
CSV_FILE = Path("bensinpriser_prices.csv")

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
    except Exception:
        return None


def normalize_date_str(raw):
    """Normalize date tokens to ISO YYYY-MM-DD. Accepts 'Idag','Igår','15/9','2025-09-15'."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    low = s.lower()
    today = date.today()
    if low.startswith("idag"):
        return today.isoformat()
    if low.startswith("igår") or low.startswith("igar") or low.startswith("ig"):
        return (today - timedelta(days=1)).isoformat()
    # ISO already?
    m_iso = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s)
    if m_iso:
        y, mo, d = m_iso.groups()
        try:
            return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
        except:
            pass
    # dd/mm
    m = re.match(r"^(\d{1,2})/(\d{1,2})$", s)
    if m:
        day = int(m.group(1)); month = int(m.group(2))
        today = date.today()
        if (month > today.month) or (month == today.month and day > today.day):
            yr = today.year - 1
        else:
            yr = today.year
        try:
            return f"{yr:04d}-{month:02d}-{day:02d}"
        except:
            return None
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
    except:
        p = None
    pstr = f"{p:.3f}" if p is not None else ""
    d = "" if date_iso is None else str(date_iso).strip()
    f = "" if fuel is None else str(fuel).strip().lower()
    return (st, pstr, d, f)


# ---------- Scrape a single URL ----------
def scrape_one_url(url, fuel_key):
    """Return list of normalized row dicts scraped from one fuel page."""
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    out = []
    today_iso = date.today().isoformat()

    for tr in soup.find_all("tr", class_="table-row"):
        tds = tr.find_all("td")
        if not tds:
            continue
        station = tds[0].get_text(strip=True)
        s_low = station.lower()
        if ("costco" in s_low or "medlemsskap krävs" in s_low or "medlemskap krævs" in s_low or "medlemskap krävs" in s_low or s_low.startswith("tips")):
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
            continue

        fk = normalize_fuel(fuel_key)
        # min/max checks
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
    return out


# ---------- Main ----------
def main():
    # Build normalized existing key set and counts per (fuel,date)
    existing_keys = set()
    counts_per_fd = {}  # (fuel_norm, date_iso) -> int, counts of existing rows
    df_old = pd.DataFrame(columns=COLS_ORDER)
    if CSV_FILE.exists():
        df_old = pd.read_csv(CSV_FILE, dtype=object)
        for _, r in df_old.iterrows():
            fuel_norm = normalize_fuel(r.get("Fuel"))
            date_iso = normalize_date_str(r.get("Date")) or r.get("Date")
            price_v = parse_number(r.get("Price"))
            key = make_key_norm(r.get("Station"), price_v, date_iso, fuel_norm)
            existing_keys.add(key)
            fd = (fuel_norm, date_iso)
            counts_per_fd[fd] = counts_per_fd.get(fd, 0) + 1

    # Collect candidates across all fuels
    candidates = []
    for fuel_key, url in FUEL_URLS.items():
        scraped = scrape_one_url(url, fuel_key)
        candidates.extend(scraped)

    # Pending tracking: NOTE -> pending_counts now tracked per fuel ONLY (not per fuel+date)
    pending_counts_by_fuel = {}    # fuel_norm -> int (pending rows accepted in this run)
    pending_keys = set()          # normalized keys accepted in this run
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

        # Price bounds sanity (again)
        if price_val < MIN_PRICE.get(fuel_norm, 0.0) or price_val > MAX_PRICE.get(fuel_norm, float("inf")):
            skipped_price += 1
            continue

        # STRICT CAP ENFORCEMENT:
        # Existing count is per (fuel, date) — that's how many rows are already stored for that date.
        existing_count = counts_per_fd.get((fuel_norm, date_iso), 0)
        # Pending count is now per-fuel ONLY (user requested): rows accepted during this run for that fuel
        pending_count = pending_counts_by_fuel.get(fuel_norm, 0)

        cap = ROW_LIMITS.get(fuel_norm, None)
        # Accept only if (existing_count + pending_count) < cap
        if cap is not None and (existing_count + pending_count) >= cap:
            skipped_cap += 1
            continue

        # Accept: register key, increment pending_counts_by_fuel and prepare for append
        pending_keys.add(key)
        pending_counts_by_fuel[fuel_norm] = pending_count + 1

        to_append.append({
            "Station": r["Station"],
            "Price": price_val,
            "Date": date_iso,
            "Fuel": fuel_norm,
            "ScrapeDate": r.get("ScrapeDate"),
            "Source": r.get("Source", "bensinpriser.nu"),
        })
        added += 1

    # Append to CSV if to_append non-empty
    if to_append:
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
    print("Summary:")
    print(f"  Added: {added}")
    print(f"  Skipped (duplicate exact match): {skipped_dup}")
    print(f"  Skipped (cap reached): {skipped_cap}")
    print(f"  Skipped (price out of bounds): {skipped_price}")
    print(f"  Total rows after run: {len(df_combined)}")
    if added:
        print("\nAdded rows:")
        for r in to_append:
            print(f" - {r['Station']} | {r['Price']} kr | {r['Date']} | {r['Fuel']}")


if __name__ == "__main__":
    main()
