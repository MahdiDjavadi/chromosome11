import os
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

    return results


if __name__ == "__main__":
    symbol = "فملی"   # مربوط به id = 1
    data = fetch_symbol_data(symbol)

    print("TYPE 0 (first 2 rows):")
    print(data[0][:2])
    
    print("\nTYPE 1 (first 2 rows):")
    print(data[1][:2])
