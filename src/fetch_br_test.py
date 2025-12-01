# src/fetch_br_test.py
import os
import json
import requests
from utils_db import upsert

# Environment variable
API_KEY = os.getenv("BRSAPI_API_KEY")
BASE_URL = "https://brsapi.ir/Api/Tsetmc/History.php"

# انتخاب یک نماد برای تست
params = {
    "key": API_KEY,
    "type": 0,  # symbol_price
    "l18": "فملی"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

def fetch_br_test():
    # Fetch data
    response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=15)
    response.raise_for_status()
    data = response.json()

    # پاکسازی و default برای رکورد ناقص
    cleaned_data = []
    for row in data:
        # اگر فیلد price موجود نبود، default 0 بگذار
        row["price"] = row.get("price", 0)
        cleaned_data.append(row)

    # ذخیره JSON intermediate
    os.makedirs("tmp", exist_ok=True)
    json_path = "tmp/fetch_test.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(cleaned_data, f, ensure_ascii=False, indent=2)
    print(f"✅ Saved {len(cleaned_data)} records to {json_path}")

    # Insert/Upsert به DB
    for row in cleaned_data:
        try:
            upsert(
                """
                INSERT INTO symbol_price (symbol, price, date, volume)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    price = VALUES(price),
                    volume = VALUES(volume)
                """,
                (
                    row.get("symbol"),
                    row.get("price"),
                    row.get("date"),
                    row.get("volume")
                )
            )
        except Exception as e:
            print(f"❌ Insert failed: {e}")

if __name__ == "__main__":
    fetch_br_test()
