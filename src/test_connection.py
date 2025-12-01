from utils_db import fetch_all

rows = fetch_all("SELECT 1 AS test;")
print("DB TEST RESULT:", rows)
