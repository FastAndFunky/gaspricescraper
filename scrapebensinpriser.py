import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date
from pathlib import Path

# -------------------------------
# Config
# -------------------------------

HEADERS = {"User-Agent": "Mozilla/5.0"}
CSV_FILE = Path("bensinpriser_prices.csv")

# URLs for different fuel types
FUEL_URLS = {
    "95 (E10)": "https://bensinpriser.nu/stationer/95/alla/alla",
    "98": "https://bensinpriser.nu/stationer/98/alla/alla",
    "Diesel": "https://bensinpriser.nu/stationer/diesel/alla/alla",
    "Etanol": "https://bensinpriser.nu/stationer/etanol/alla/alla"
}

# Price sanity thresholds
MIN_PRICE = {
    "95 (E10)": 14.0,
    "98": 14.0,
    "Diesel": 14.0,
    "Etanol": 11.0
}

# Row limits per day per fuel
ROW_LIMITS = {
    "95 (E10)": 3,
    "Etanol": 3,
    "98": 10,
    "Diesel": 10
}


# -------------------------------
# Helpers
# -------------------------------

def parse_number(s: str):
    """Convert strings like '14,71kr' into a float."""
    if not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    cleaned = re.sub(r"[^\d,.-]", "", s).replace(" ", "")
    if "," in cleaned and "." not in cleaned:
        cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def normalize_date(d: str) -> str:
    """Convert '15/9' into 'YYYY-MM-DD', detecting correct year."""
    if not d:
        return None
    try:
        day, month = map(int, d.split("/"))
        today = date.today()
        # If scraped month/day is in the future, assume last year
        if month > today.month or (month == today.month and day > today.day):
            year = today.year - 1
        else:
            year = today.year
        return f"{year}-{month:02d}-{day:02d}"
    except Exception:
        return None


# -------------------------------
# Scraping
# -------------------------------

def scrape_one_url(url: str, fuel_type: str):
    """Scrape all station rows from a given bensinpriser.nu fuel URL."""
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    rows_data = []
    for tr in soup.find_all("tr", class_="table-row"):
        tds = tr.find_all("td")
        if not tds:
            continue

        station_name = tds[0].get_text(strip=True)

        # Exclude stations that require membership or are just "TIPS!"
        name_lower = station_name.lower()
        if (
            "costco" in name_lower
            or "medlemsskap krävs" in name_lower
            or "medlemskap krävs" in name_lower
            or name_lower.startswith("tips")
        ):
            continue

        # Extract price and date from second <td>
        price_value, row_date = None, None
        if len(tds) > 1:
            price_td = tds[1]
            small = price_td.find("small")
            if small:
                row_date = normalize_date(small.get_text(strip=True))
                small.extract()
            price_text = price_td.get_text(strip=True)
            price_value = parse_number(price_text)

        # Discard invalid / nonsense prices
        if price_value is None or row_date is None:
            continue
        if price_value < MIN_PRICE.get(fuel_type, 0):
            continue

        row = {
            "Station": station_name,
            "Price": price_value,
            "Date": row_date,
            "Fuel": fuel_type,
            "ScrapeDate": date.today().isoformat(),
            "Source": "bensinpriser.nu",
        }
        rows_data.append(row)

    return pd.DataFrame(rows_data)


def scrape_all():
    """Scrape all fuels defined in FUEL_URLS and apply per-fuel limits."""
    frames = []
    for fuel_type, url in FUEL_URLS.items():
        df = scrape_one_url(url, fuel_type)
        if not df.empty:
            # Apply row limit per (Fuel, Date)
            limit = ROW_LIMITS.get(fuel_type, None)
            if limit:
                df = (
                    df.groupby(["Fuel", "Date"], group_keys=False)
                    .head(limit)
                    .reset_index(drop=True)
                )
            frames.append(df)
    if frames:
        return pd.concat(frames, ignore_index=True)
    else:
        return pd.DataFrame()


# -------------------------------
# Save to CSV
# -------------------------------

def save_to_csv(df_new, file=CSV_FILE):
    """Save new data to CSV, ensuring no duplicates and correct summary."""
    if df_new.empty:
        print("No data scraped.")
        return

    # Ensure consistent column order
    cols_order = ["Station", "Price", "Date", "Fuel", "ScrapeDate", "Source"]
    for c in cols_order:
        if c not in df_new.columns:
            df_new[c] = None
    df_new = df_new[cols_order]

    if file.exists():
        df_old = pd.read_csv(file, dtype=object)

        # Count unique rows before adding
        before = df_old.drop_duplicates(subset=["Station", "Price", "Date", "Fuel"])
        before_len = len(before)
        print("Before len: " + str(before_len))

        # Combine old + new
        combined = pd.concat([df_old, df_new], ignore_index=True)

        # Deduplicate after adding
        after = combined.drop_duplicates(subset=["Station", "Price", "Date", "Fuel"], keep="first")
        after_len = len(after)
        print("After len: " + str(after_len))

        # Truly new rows = difference
        added = after_len - before_len
        duplicates = len(df_new) - added

        # Save
        after.to_csv(file, index=False)

        print(f"✅ Added {added} new rows, skipped {duplicates} duplicates. Total rows: {after_len}")
    else:
        df_new.drop_duplicates(subset=["Station", "Price", "Date", "Fuel"], keep="first").to_csv(file, index=False)
        print(f"Created {file} with {len(df_new)} rows.")



# -------------------------------
# Main
# -------------------------------

if __name__ == "__main__":
    df_all = scrape_all()
    save_to_csv(df_all)
