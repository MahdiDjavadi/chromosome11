import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("BRSAPI_API_KEY")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}

def fetch_symbol_data(symbol):
    results = {}

    for t in [0, 1]:
        url = f"https://brsapi.ir/Api/Tsetmc/History.php?key={API_KEY}&type={t}&l18={symbol}"
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()

        results[t] = r.json()

        # save json inside /data
        os.makedirs("data", exist_ok=True)
        file_path = f"data/{symbol}_{t}.json"
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(results[t], f, ensure_ascii=False, indent=2)

        print(f"✔️ Saved: {file_path}")

    return results


if __name__ == "__main__":
    symbol = "فملی"  # id=1
    fetch_symbol_data(symbol)
