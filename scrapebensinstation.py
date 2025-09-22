#!/usr/bin/env python3
"""
scrape_bensinstation.py

Simple scraper for https://www.bensinstation.nu/ that:
 - extracts the full <table class="priceTable"> (all rows),
 - creates a DataFrame with scraped rows,
 - appends only truly new rows to bensinstation_prices.csv (unique by Station+Price+Date+Fuel when available),
 - prints an accurate summary.
"""

import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date
from pathlib import Path
from typing import List, Optional

# -----------------------
# Config
# -----------------------
URL = "https://www.bensinstation.nu/"
HEADERS = {"User-Agent": "Mozilla/5.0"}
CSV_FILE = Path("bensinstation_prices.csv")

# keywords to locate important columns
STATION_KWS = ["station", "namn", "name"]
PRICE_KWS = ["pris", "price", "kr", "price/l", "pris/l", "price per"]
DATE_KWS = ["date", "datum", "dag"]

# -----------------------
# Helpers
# -----------------------
def find_column(columns: List[str], keywords: List[str]) -> Optional[str]:
    """Return the first column name containing any keyword (case-insensitive)"""
    lowered = [c.lower() for c in columns]
    for kw in keywords:
        for i, cl in enumerate(lowered):
            if kw in cl:
                return columns[i]
    return None


def parse_number(s: str) -> Optional[float]:
    """Parse localized numbers like '19,34', '19,34kr', '1 234,56' -> float"""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    cleaned = re.sub(r"[^\d,.-]", "", s).replace(" ", "")
    # interpret comma as decimal separator if no dot present
    if "," in cleaned and "." not in cleaned:
        cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except Exception:
        return None


def normalize_date_from_small(d: str) -> Optional[str]:
    """Convert '15/9' into 'YYYY-MM-DD' using simple year inference like before."""
    if not d or not isinstance(d, str):
        return None
    try:
        day, month = map(int, d.split("/"))
    except Exception:
        return None
    today = date.today()
    if (month > today.month) or (month == today.month and day > today.day):
        year = today.year - 1
    else:
        year = today.year
    return f"{year}-{month:02d}-{day:02d}"


def make_key(station: str, price, date_str: str, fuel: Optional[str]) -> tuple:
    """Normalized key for uniqueness checks"""
    s = "" if station is None else str(station).strip().lower()
    f = "" if fuel is None else str(fuel).strip().lower()
    try:
        p = float(price) if price is not None else None
    except Exception:
        p = None
    p_str = f"{p:.3f}" if p is not None else ""
    d = "" if date_str is None else str(date_str).strip()
    return (s, p_str, d, f)


# -----------------------
# Scraping
# -----------------------
def scrape_table():
    """Scrape the priceTable and return a DataFrame with extracted rows."""
    resp = requests.get(URL, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    table = soup.find("table", {"class": "priceTable"})
    if not table:
        raise RuntimeError("No <table class='priceTable'> found on page")

    # try to read headers from the table <thead>
    thead = table.find("thead")
    if thead:
        headers = [th.get_text(strip=True) for th in thead.find_all("th")]
    else:
        headers = []

    # gather all <tr> in tbody (or directly under table if no tbody)
    tbody = table.find("tbody") or table
    rows: List[List[str]] = []
    for tr in tbody.find_all("tr"):
        # only keep rows that have <td> cells
        tds = tr.find_all("td")
        if not tds:
            continue

        # Extract each cell text (but remove any <small> date from price cell first)
        cells = []
        for td in tds:
            # if there is a <small> inside this td (date), remove it from the td text
            smalls = td.find_all("small")
            for s in smalls:
                # do not lose the date — we'll extract date from first small we find later
                s.extract()
            cells.append(td.get_text(strip=True))
        rows.append(cells)

    # if headers were not found, generate generic column names
    if not headers:
        # create column names col0..colN-1 (N = max cells in any row)
        max_cols = max(len(r) for r in rows) if rows else 0
        headers = [f"col{i}" for i in range(max_cols)]

    # Build DataFrame and ensure consistent columns length
    df = pd.DataFrame(rows, columns=headers[: len(rows[0])] if headers else None)

    # Add ScrapeDate and Source
    df["ScrapeDate"] = date.today().isoformat()
    df["Source"] = "bensinstation.nu"

    # Try to capture Date from any <small> tags left inside rows (some pages put date in a separate cell)
    # We re-parse row tds to find <small> values explicitly to avoid mixing them into price cells.
    # To be safe, we iterate row by row and re-parse the original tr elements.
    # (This ensures we have the correct Date value, if present.)
    # Re-run request and map smalls per row again
    # NOTE: we purposely re-iterate the tbody tr list to fetch small text because earlier we removed smalls.
    # Build list of dates (one per scraped row) by scanning the original tr nodes again.
    dates = []
    tr_list = list(tbody.find_all("tr"))
    # align tr_list and rows: only consider trs that had <td>
    tr_with_td = [tr for tr in tr_list if tr.find_all("td")]
    for tr in tr_with_td:
        # find first <small> present inside that tr (if any)
        small = tr.find("small")
        if small:
            raw = small.get_text(strip=True)
            dates.append(normalize_date_from_small(raw))
        else:
            dates.append(None)

    # Attach Date column: prefer a header named Date/Datum if it exists, else add Date column
    date_col_name = None
    for candidate in ("Date", "Datum"):
        if candidate in df.columns:
            date_col_name = candidate
            break
    # if a date header already existed we won't overwrite it, otherwise add our parsed dates
    if date_col_name is None:
        # append Date column based on parsed smalls (len may match)
        # if count mismatches, we will fill with None where missing
        if len(dates) == len(df):
            df["Date"] = dates
        else:
            # fallback: simply None for all, better than misalignment
            df["Date"] = [None] * len(df)
    else:
        # ensure Date column exists and standardized where possible
        try:
            df[date_col_name] = df[date_col_name].apply(lambda v: normalize_date_from_small(v) if isinstance(v, str) and "/" in v else v)
        except Exception:
            pass

    return df


# -----------------------
# Save with correct dedupe and accurate summary
# -----------------------
def save_to_csv(df_new: pd.DataFrame, file: Path = CSV_FILE):
    """
    Append only truly new rows to CSV. Use unique key (Station, Price, Date, Fuel)
    where those columns exist; otherwise fall back to sensible defaults.
    Provides an accurate summary of added vs duplicates.
    """
    if df_new is None or df_new.empty:
        print("No data scraped.")
        return

    # detect likely column names in this DataFrame
    cols = list(df_new.columns)
    station_col = find_column(cols, STATION_KWS) or (cols[0] if cols else None)
    price_col = find_column(cols, PRICE_KWS)
    date_col = find_column(cols, DATE_KWS) or "Date"
    # Fuel may not be present on this site; set to None if not present
    fuel_col = "Fuel" if "Fuel" in cols else None

    # ensure expected columns exist
    for expected in [station_col, price_col, date_col, fuel_col]:
        if expected and expected not in df_new.columns:
            df_new[expected] = None

    # Normalize price to numeric for consistent comparison
    if price_col and price_col in df_new.columns:
        df_new[price_col] = pd.to_numeric(df_new[price_col].apply(lambda x: parse_number(x) if pd.notna(x) else None), errors="coerce")

    # Build existing key set from CSV if exists
    existing_keys = set()
    if file.exists():
        df_old = pd.read_csv(file, dtype=object)

        # normalize old price to numeric if possible
        if price_col and price_col in df_old.columns:
            df_old[price_col] = pd.to_numeric(df_old[price_col], errors="coerce")

        for _, r in df_old.iterrows():
            key = make_key(r.get(station_col), r.get(price_col), r.get(date_col), r.get(fuel_col))
            existing_keys.add(key)
    else:
        df_old = pd.DataFrame(columns=df_new.columns)

    # iterate new rows and collect only truly new rows
    to_add = []
    duplicates = 0
    for _, r in df_new.iterrows():
        key = make_key(r.get(station_col), r.get(price_col), r.get(date_col), r.get(fuel_col))
        if key in existing_keys:
            duplicates += 1
            continue
        existing_keys.add(key)  # avoid duplicates within this run
        to_add.append(r.to_dict())

    added = len(to_add)

    if added == 0:
        print(f"ℹ️ No new rows added. Skipped {duplicates} duplicates. Total rows: {len(df_old)}")
        return

    # Append the new rows and save
    df_to_append = pd.DataFrame(to_add)
    # preserve old columns order if possible, else use new frame
    combined = pd.concat([df_old, df_to_append], ignore_index=True, sort=False)

    # final sanity dedupe (shouldn't remove anything)
    combined = combined.drop_duplicates(subset=[station_col, price_col, date_col] if price_col else None, keep="first")

    # Reorder columns: prefer keeping original table header order, but ensure ScrapeDate & Source present
    if "ScrapeDate" not in combined.columns:
        combined["ScrapeDate"] = date.today().isoformat()
    if "Source" not in combined.columns:
        combined["Source"] = "bensinstation.nu"

    # Save
    combined.to_csv(file, index=False)
    print(f"✅ Added {added} new rows, skipped {duplicates} duplicates. Total rows: {len(combined)}")
    # print details of added rows
    for r in to_add:
        st = r.get(station_col, "")
        pr = r.get(price_col, "")
        dt = r.get(date_col, "")
        fu = r.get(fuel_col, "")
        print(f" - {st} | {pr} | {dt} | {fu}")


# -----------------------
# Main
# -----------------------
if __name__ == "__main__":
    df = scrape_table()
    # quick check that we captured multiple rows if present
    # (no verbose printing of the whole table)
    print(f"Scraped {len(df)} rows from {URL}")
    save_to_csv(df)
