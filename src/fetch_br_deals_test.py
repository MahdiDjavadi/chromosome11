import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://brsapi.ir/Api/Tsetmc/History.php"
API_KEY = os.getenv("BRSAPI_API_KEY")
SYMBOL = "فملی"
TYPE = 1  # معاملات

def fetch_deals():
    url = f"{BASE_URL}?key={API_KEY}&type={TYPE}&l18={SYMBOL}"
    print("Requesting:", url)

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()

    os.makedirs("tmp", exist_ok=True)
    with open("tmp/fetch_deals_test.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("Saved tmp/fetch_deals_test.json")
    print(f"Records: {len(data)}")

if __name__ == "__main__":
    fetch_deals()
