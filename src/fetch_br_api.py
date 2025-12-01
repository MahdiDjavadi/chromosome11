# src/fetch_br_api.py
import os
import requests
import json
from db import get_connection  # همان db.py که SSL و CI درست دارد

# Mapping type → table
TABLE_MAP = {
    0: "symbol_price",
    1: "symbol_deals"
}

# User-Agent واقعی برای جلوگیری از بلاک شدن
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
}

def fetch_br_api(type_id: int):
    if type_id not in TABLE_MAP:
        raise ValueError("Invalid type_id, must be 0 or 1")

    url = f"https://api.brs.example.com/data?type={type_id}"  # جایگزین با لینک واقعی
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    data = response.json()  # فرض JSON Array از دیکشنری‌ها

    table_name = TABLE_MAP[type_id]

    conn = get_connection()
    cursor = conn.cursor()

    # نمونه ساده insert/update با ON DUPLICATE KEY
    for row in data:
        columns = ", ".join(row.keys())
        placeholders = ", ".join(["%s"] * len(row))
        update_assign = ", ".join([f"{col}=VALUES({col})" for col in row.keys()])
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) " \
              f"ON DUPLICATE KEY UPDATE {update_assign}"
        cursor.execute(sql, list(row.values()))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Fetched and upserted {len(data)} rows into {table_name}")


if __name__ == "__main__":
    # تست سریع
    fetch_br_api(0)  # symbol_price
    fetch_br_api(1)  # symbol_deals
