import os
import json
import time
import requests
from urllib.parse import quote
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://brsapi.ir/Api/Tsetmc/History.php"
API_KEY = os.getenv("BRSAPI_API_KEY")
SYMBOL = "فملی"
TYPE = 1  # معاملات

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def fetch_deals():
    symbol_encoded = quote(SYMBOL)
    url = f"{BASE_URL}?key={API_KEY}&type={TYPE}&curl=1&l18={symbol_encoded}"

    print("Requesting:", url)

    for attempt in range(5):
        try:
            response = requests.get(url, headers=HEADERS, timeout=20)
            response.raise_for_status()

            data = response.json()

            os.makedirs("tmp", exist_ok=True)
            with open("tmp/fetch_deals_test.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            print("Saved tmp/fetch_deals_test.json")
            print(f"Records: {len(data)}")
            return

        except Exception as e:
            print(f"Attempt {attempt+1}/5 failed:", e)
            time.sleep(2)

    print("❌ API refuses connection after retries.")

if __name__ == "__main__":
    fetch_deals()
