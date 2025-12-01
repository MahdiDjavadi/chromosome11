from utils_db import fetch_all

def test():
    rows = fetch_all("SELECT 1 AS test;")
    if rows:
        print("✅ Test query success:", rows)
    else:
        print("❌ Test query failed.")

if __name__ == "__main__":
    test()
