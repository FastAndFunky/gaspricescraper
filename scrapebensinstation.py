import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import os

CSV_FILE = "bensinstation_prices.csv"
URL = "https://www.bensinstation.nu/"

def normalize_date(raw_date: str) -> str:
    """Convert 'Idag', 'Igår' or DD/MM into YYYY-MM-DD format."""
    today = datetime.today().date()
    yesterday = today - timedelta(days=1)
    year = today.year

    raw_date = raw_date.strip().lower()
    if "idag" in raw_date:
        return today.strftime("%Y-%m-%d")
    elif "igår" in raw_date:
        return yesterday.strftime("%Y-%m-%d")
    else:
        match = re.match(r"(\d{1,2})/(\d{1,2})", raw_date)
        if match:
            day, month = map(int, match.groups())
            return datetime(year, month, day).strftime("%Y-%m-%d")
    return raw_date

def scrape_bensinstation():
    response = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", {"class": "priceTable"})
    rows = table.find("tbody").find_all("tr")

    data = []
    today = datetime.today().strftime("%Y-%m-%d")  # ScrapeDate

    for row in rows:
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        if len(cols) < 4:
            continue

        bolag = cols[0]
        bensinpris = cols[1] if cols[1] else ""
        dieselpris = cols[2] if cols[2] else ""
        etanol = cols[3] if cols[3] else ""
        datum = normalize_date(cols[-1])

        data.append([bolag, bensinpris, dieselpris, etanol, datum, today])
    return data

def save_to_csv(data):
    new_df = pd.DataFrame(data, columns=["Bolag","Bensinpris","Dieselpris","Etanol","Datum","ScrapeDate"])

    if os.path.exists(CSV_FILE):
        old_df = pd.read_csv(CSV_FILE)

        # Merge datasets
        combined = pd.concat([old_df, new_df], ignore_index=True)

        # Deduplicate based on key columns only
        before = len(combined)
        combined = combined.drop_duplicates(subset=["Bolag","Bensinpris","Dieselpris","Datum"], keep="first")
        after = len(combined)

        added_rows = after - len(old_df)
        skipped = len(new_df) - added_rows
    else:
        combined = new_df
        added_rows = len(new_df)
        skipped = 0

    combined.to_csv(CSV_FILE, index=False, encoding="utf-8")

    print(f"✅ bensinstation.nu scraped")
    print(f"   ➕ {added_rows} new rows added")
    print(f"   ➖ {skipped} duplicates skipped")
    print(f"   📊 Final CSV length: {len(combined)}")

if __name__ == "__main__":
    scraped_data = scrape_bensinstation()
    save_to_csv(scraped_data)
