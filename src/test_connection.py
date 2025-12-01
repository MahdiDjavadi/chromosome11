from utils_db import fetch_all

if __name__ == "__main__":
    rows = fetch_all("SELECT 1 AS test;")
    if rows:
        print("✅ Test query successful:", rows)
    else:
        print("❌ Test query failed.")
