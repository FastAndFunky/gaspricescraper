#!/usr/bin/env python3
"""
scrapebensinstation.py

- Scrapes https://www.bensinstation.nu/ priceTable
- Enforces CSV schema: Bolag,Bensinpris,Dieselpris,Etanol,Datum,ScrapeDate
- Normalizes Datum universally to ISO YYYY-MM-DD (handles 'Idag','Igår','i förrgår',
  dd/mm, dd.mm, dd-mm, Swedish month names, ISO, etc.)
- Never deletes/replaces existing rows. Appends only new rows whose key
  (Bolag, Bensinpris, Dieselpris, Datum) does not already exist.
- Prints a summary of scraped/skipped/added rows.
"""

from pathlib import Path
from datetime import datetime, timedelta, date
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
from typing import Optional

# Config
URL = "https://www.bensinstation.nu/"
CSV_FILE = Path("bensinstation_prices.csv")
DESIRED_COLS = ["Bolag", "Bensinpris", "Dieselpris", "Etanol", "Datum", "ScrapeDate"]
HEADERS = {"User-Agent": "Mozilla/5.0"}

# Swedish month name mapping (short + full) -> month number
_MONTHS = {
    "jan": 1, "januari": 1,
    "feb": 2, "februari": 2,
    "mar": 3, "mars": 3,
    "apr": 4, "april": 4,
    "maj": 5,
    "jun": 6, "juni": 6,
    "jul": 7, "juli": 7,
    "aug": 8, "augusti": 8,
    "sep": 9, "sept": 9, "september": 9,
    "okt": 10, "oktober": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


# ---------- helpers ----------
def _clean_token(s: str) -> str:
    """Lowercase, normalize whitespace and remove non-essential punctuation for token matching."""
    if s is None:
        return ""
    s2 = str(s).strip().lower()
    # normalize some unicode variants
    s2 = s2.replace("å", "å").replace("ä", "ä").replace("ö", "ö")
    s2 = re.sub(r"\s+", " ", s2)
    return s2


def normalize_date_token(raw_date: str) -> Optional[str]:
    """
    Universal date normalizer.

    Accepts:
      - "Idag", "idag", "Idag kl 12", "Today"
      - "Igår", "igår", "I går", "igar"
      - "i förrgår", "förrgår" (day before yesterday)
      - "15/9", "15.9", "15-9", optionally with year: "15/9/2025" or "15-9-25"
      - "2025-09-15" or "2025.09.15"
      - "15 september", "15 sept", "15 sept 2025"
      - With minor noise around (e.g. "Datum: 15/9", "15/9 kl. 08:00")

    Returns ISO date string "YYYY-MM-DD" or None if unable to parse.
    """
    if raw_date is None:
        return None
    s = str(raw_date).strip()
    if not s:
        return None

    low = s.lower().replace("\xa0", " ").strip()

    # Handle relative words in Swedish / English
    # Normalize tokens without diacritics for loose matching
    low_nodiac = low.replace("å", "a").replace("ä", "a").replace("ö", "o")
    if "idag" in low_nodiac or "today" in low_nodiac:
        return date.today().isoformat()
    if "igår" in low_nodiac or "igar" in low_nodiac or "yesterday" in low_nodiac:
        return (date.today() - timedelta(days=1)).isoformat()
    # day before yesterday variants: "förrgår", "i förrgår", "forrgor"(typo), "förrgår"
    if "förrgår" in low or "forrgor" in low or "i förrgår" in low or "i forrgor" in low or "i forrgår" in low or "i forrgar" in low:
        return (date.today() - timedelta(days=2)).isoformat()
    if "förrgår" in low_nodiac or "forrgor" in low_nodiac:
        return (date.today() - timedelta(days=2)).isoformat()
    # additionally support short Swedish forms like "i forrgar" (typo tolerant)
    if re.search(r"\bforr", low_nodiac):
        # a fallback to day-before-yesterday if token contains forr...
        return (date.today() - timedelta(days=2)).isoformat()

    # Strip common prefixes/suffixes
    s_clean = re.sub(r"^(datum[:\s-]*)", "", low, flags=re.IGNORECASE).strip()
    s_clean = re.sub(r"\bkl\.?\s*\d{1,2}(:\d{2})?\b", "", s_clean).strip()  # remove "kl 08" times
    s_clean = s_clean.replace(".", "/")  # unify . and / to /
    s_clean = s_clean.replace("-", "/")  # unify - and / to /
    s_clean = s_clean.replace(",", " ").strip()

    # Try ISO YYYY-MM-DD or YYYY/MM/DD
    m_iso = re.match(r"^(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})$", s_clean)
    if m_iso:
        y, mo, d = int(m_iso.group(1)), int(m_iso.group(2)), int(m_iso.group(3))
        try:
            return datetime(year=y, month=mo, day=d).date().isoformat()
        except Exception:
            pass

    # Try dd/mm or dd/mm/yy(yy)
    m_dm = re.match(r"^(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?$", s_clean)
    if m_dm:
        day = int(m_dm.group(1)); month = int(m_dm.group(2))
        year_token = m_dm.group(3)
        today_dt = date.today()
        if year_token:
            yr = int(year_token)
            if yr < 100:  # two-digit year
                # interpret 20xx for years < 100
                yr += 2000
        else:
            # infer year: if month/day is after today => last year else this year
            if (month > today_dt.month) or (month == today_dt.month and day > today_dt.day):
                yr = today_dt.year - 1
            else:
                yr = today_dt.year
        try:
            return datetime(year=yr, month=month, day=day).date().isoformat()
        except Exception:
            pass

    # Try "15 september" or "15 sept 2025"
    m_text = re.match(r"^(\d{1,2})\s+([a-zåäö\.]+)(?:\s+(\d{2,4}))?$", s_clean)
    if m_text:
        day = int(m_text.group(1))
        month_token = m_text.group(2).strip().rstrip(".")
        year_token = m_text.group(3)
        month_key = month_token.lower()
        # normalize month token (short forms)
        month_num = None
        # match English month names too if present
        month_key_short = month_key[:3]
        for k, v in _MONTHS.items():
            if month_key == k or month_key_short == k[:3]:
                month_num = v
                break
        # also check full Swedish names mapping
        if month_num is None:
            month_key2 = month_key.replace("ä", "a").replace("ö", "o")
            for k, v in _MONTHS.items():
                if month_key2.startswith(k[:3]):
                    month_num = v
                    break
        if month_num:
            if year_token:
                yr = int(year_token)
                if yr < 100:
                    yr += 2000
            else:
                today_dt = date.today()
                if (month_num > today_dt.month) or (month_num == today_dt.month and day > today_dt.day):
                    yr = today_dt.year - 1
                else:
                    yr = today_dt.year
            try:
                return datetime(year=yr, month=month_num, day=day).date().isoformat()
            except Exception:
                pass

    # Fallback: try to find a dd number and month number anywhere
    m_any = re.search(r"(\d{1,2})\D+(\d{1,2})", s_clean)
    if m_any:
        day = int(m_any.group(1)); month = int(m_any.group(2))
        today_dt = date.today()
        if (month > today_dt.month) or (month == today_dt.month and day > today_dt.day):
            yr = today_dt.year - 1
        else:
            yr = today_dt.year
        try:
            return datetime(year=yr, month=month, day=day).date().isoformat()
        except Exception:
            pass

    # As last resort, return None (but in practice this should rarely happen)
    return None


def make_key(bolag, bensinpris, dieselpris, datum) -> tuple:
    """
    Normalized uniqueness key for duplicate detection:
    Using textual matching (strip) as requested: Bolag, Bensinpris, Dieselpris, Datum
    """
    b_k = "" if bolag is None else str(bolag).strip()
    ben_k = "" if bensinpris is None else str(bensinpris).strip()
    die_k = "" if dieselpris is None else str(dieselpris).strip()
    d_k = "" if datum is None else str(datum).strip()
    return (b_k, ben_k, die_k, d_k)


# ---------- scraping ----------
def scrape_table() -> list[list[str]]:
    """
    Scrape the priceTable and return rows as lists:
    [Bolag, Bensinpris, Dieselpris, Etanol, Datum]
    """
    resp = requests.get(URL, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    table = soup.find("table", {"class": "priceTable"})
    if table is None:
        raise RuntimeError("No <table class='priceTable'> found on page")

    tbody = table.find("tbody") or table
    rows_out = []

    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        # Remove <small> from td text so price cells are clean; keep small for separate parsing
        cells = []
        for td in tds:
            # extract smalls to avoid them polluting price text
            smalls = td.find_all("small")
            for s in smalls:
                s.extract()
            cells.append(td.get_text(strip=True))

        if len(cells) < 4:
            continue

        bolag = cells[0]
        bensinpris = cells[1] if len(cells) > 1 else ""
        dieselpris = cells[2] if len(cells) > 2 else ""
        etanol = cells[3] if len(cells) > 3 else ""

        # Now find date: prefer an explicit <small> tag inside the tr if present (common pattern)
        small_tag = tr.find("small")
        if small_tag:
            raw_date = small_tag.get_text(strip=True)
            datum_iso = normalize_date_token(raw_date) or raw_date
        else:
            # fallback: try last cell (some tables place date as last column)
            possible = cells[-1]
            datum_iso = normalize_date_token(possible) or possible

        rows_out.append([bolag, bensinpris, dieselpris, etanol, datum_iso])

    return rows_out


# ---------- save/append logic (never deletes existing rows) ----------
def save_rows_scraped(scraped_rows: list[list[str]]):
    """
    Append only rows which are not already present in CSV based on the key:
      (Bolag, Bensinpris, Dieselpris, Datum)
    Old rows are never removed or replaced.
    """

    scrape_date = date.today().isoformat()

    # Load existing CSV and build existing key set
    existing_keys = set()
    if CSV_FILE.exists():
        df_old = pd.read_csv(CSV_FILE, dtype=str).fillna("")
        for _, r in df_old.iterrows():
            key = make_key(r.get("Bolag"), r.get("Bensinpris"), r.get("Dieselpris"), r.get("Datum"))
            existing_keys.add(key)
    else:
        df_old = pd.DataFrame(columns=DESIRED_COLS)

    to_add = []
    skipped = 0
    scraped_total = 0

    for row in scraped_rows:
        scraped_total += 1
        bolag, bensinpris, dieselpris, etanol, datum = row

        # Ensure Datum is ISO if possible
        datum_iso = datum
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", str(datum)):
            parsed = normalize_date_token(datum)
            datum_iso = parsed if parsed else str(datum).strip()

        key = make_key(bolag, bensinpris, dieselpris, datum_iso)
        if key in existing_keys:
            skipped += 1
            continue

        # Prepare row to append and register key immediately to avoid duplicates within same run
        to_add.append({
            "Bolag": bolag,
            "Bensinpris": bensinpris,
            "Dieselpris": dieselpris,
            "Etanol": etanol,
            "Datum": datum_iso,
            "ScrapeDate": scrape_date
        })
        existing_keys.add(key)

    added = len(to_add)

    # Append new rows without deleting old rows
    if added > 0:
        df_new = pd.DataFrame(to_add, columns=DESIRED_COLS)
        df_out = pd.concat([df_old, df_new], ignore_index=True, sort=False)
        # Ensure columns exist and are in requested order
        for c in DESIRED_COLS:
            if c not in df_out.columns:
                df_out[c] = ""
        df_out = df_out[DESIRED_COLS]
        df_out.to_csv(CSV_FILE, index=False, encoding="utf-8")
    else:
        df_out = df_old

    # Summary
    print("bensinstation.nu scrape summary:")
    print(f"  Scraped rows found on page : {scraped_total}")
    print(f"  Rows skipped as duplicates : {skipped}")
    print(f"  New rows appended         : {added}")
    print(f"  Final CSV length          : {len(df_out)}")


# ---------- main ----------
def main():
    scraped = scrape_table()
    save_rows_scraped(scraped)


if __name__ == "__main__":
    main()
