# src/fetch_br_test.py
import os
import requests
from utils_db import upsert, get_connection  # فرض بر این است utils_db همان کد پایدار دیتابیس شماست
from dotenv import load_dotenv

load_dotenv()  # بارگذاری .env

API_KEY = os.getenv("BRSAPI_API_KEY")
SYMBOL = "فملی"  # یک نماد برای تست
TYPE = 0  # یا 1
URL = f"https://brsapi.ir/Api/Tsetmc/History.php?key={API_KEY}&type={TYPE}&l18={SYMBOL}"

HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36 OPR/106.0.0.0",
        "Accept": "application/json, text/plain, */*"
}

def fetch_and_insert():
    try:
        response = requests.get(URL, headers=HEADERS, timeout=10)
        response.raise_for_status()  # اگر status != 200 باشد Exception ایجاد می‌کند

        data = response.json()  # فرض بر این است که API JSON می‌دهد
        print("✅ Fetch successful:", data)

        # فقط یک record insert/upsert برای تست
        if data:
            sample = data[0]  # فقط اولین آیتم
            query = """
            INSERT INTO symbol_price (symbol, date, price, volume)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            price = VALUES(price),
            volume = VALUES(volume)
            """
            params = (SYMBOL, sample["date"], sample["price"], sample["volume"])
            upsert(query, params)
            print("✅ Inserted/Updated one record for test.")
        else:
            print("⚠️ No data received.")

    except requests.exceptions.RequestException as e:
        print("❌ Fetch failed:", e)
    except Exception as e:
        print("❌ Insert failed:", e)

if __name__ == "__main__":
    fetch_and_insert()
