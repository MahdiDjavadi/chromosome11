# src/etl.py  (GitHub CI Version)

import os
import json
import time
import logging
from typing import List, Dict, Any
from urllib.parse import quote

import backoff
import requests
from src.db import get_connection

# ---------------------------
# PATHS (GitHub-safe)
# ---------------------------
REPO_ROOT = os.path.dirname(os.path.abspath(__file__))  # src/
REPO_ROOT = os.path.dirname(REPO_ROOT)  # project root

SYMBOLS_FILE = os.path.join(REPO_ROOT, "data", "symbols.txt")
SYMBOL_IDS_FILE = os.path.join(REPO_ROOT, "data", "symbol_ids.json")
DATA_DIR = os.path.join(REPO_ROOT, "data")

# ---------------------------
# CONFIG (From GitHub Secrets)
# ---------------------------
API_KEY = os.getenv("BRSAPI_API_KEY")
YEAR_FILTER = os.getenv("YEAR_FILTER", "1404")
SAVE_JSON = os.getenv("SAVE_JSON", "true").lower() in ("1", "true")
RATE_LIMIT_SECONDS = float(os.getenv("RATE_LIMIT_SECONDS", "0.30"))
CURL_FALLBACK = os.getenv("CURL_FALLBACK", "true").lower() in ("1", "true")
BATCH_COMMIT = os.getenv("BATCH_COMMIT_PER_SYMBOL", "true").lower() in ("1", "true")

HEADERS = {
    "User-Agent": os.getenv(
        "ETL_USER_AGENT",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}

BASE_URL = "https://brsapi.ir/Api/Tsetmc/History.php"

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s: %(message)s"
)
logger = logging.getLogger("ETL")


# ---------------------------
# Load Symbols
# ---------------------------
def load_symbols() -> List[str]:
    if os.path.exists(SYMBOLS_FILE):
        with open(SYMBOLS_FILE, "r", encoding="utf-8") as f:
            return [x.strip() for x in f if x.strip()]
    logger.error("symbols.txt not found.")
    return []


# ---------------------------
# Fetch API data
# ---------------------------
@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def fetch_type(symbol: str, t: int):
    curl_flag = "&curl=1" if CURL_FALLBACK else ""
    url = f"{BASE_URL}?key={API_KEY}&type={t}{curl_flag}&l18={quote(symbol)}"

    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    if isinstance(data, list):
        return data

    # if API returns {"key": [...]} 
    for v in data.values():
        if isinstance(v, list):
            return v

    return []


# ---------------------------
# Helpers
# ---------------------------
def ensure_numeric(v, cast=int):
    try:
        if v is None or v == "":
            return None
        return cast(v)
    except:
        try:
            return cast(float(v))
        except:
            return None


def filter_by_year(records, prefix):
    return [r for r in records if isinstance(r.get("date"), str) and r["date"].startswith(prefix)]


# ---------------------------
# SQL Inserts
# ---------------------------
def insert_prices(conn, sid, rows):
    if not rows:
        return

    q = """
    REPLACE INTO symbol_price (
        symbol_id, date, time, tno, tvol, tval,
        pmin, pmax, py, pf, pl, plc, plp,
        pc, pcc, pcp
    ) VALUES (
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s
    )
    """

    params = [
        (
            sid,
            r.get("date"),
            r.get("time"),
            ensure_numeric(r.get("tno")),
            ensure_numeric(r.get("tvol")),
            ensure_numeric(r.get("tval")),
            ensure_numeric(r.get("pmin")),
            ensure_numeric(r.get("pmax")),
            ensure_numeric(r.get("py")),
            ensure_numeric(r.get("pf")),
            ensure_numeric(r.get("pl")),
            ensure_numeric(r.get("plc")),
            ensure_numeric(r.get("plp"), float),
            ensure_numeric(r.get("pc")),
            ensure_numeric(r.get("pcc")),
            ensure_numeric(r.get("pcp"), float),
        )
        for r in rows
    ]

    cur = conn.cursor()
    cur.executemany(q, params)
    if BATCH_COMMIT:
        conn.commit()
    cur.close()

    logger.info("Inserted %d prices for %s", len(rows), sid)


def insert_deals(conn, sid, rows):
    if not rows:
        return

    q = """
    REPLACE INTO symbol_deals (
        symbol_id, date,
        Buy_CountI, Buy_CountN, Sell_CountI, Sell_CountN,
        Buy_I_Volume, Buy_N_Volume, Sell_I_Volume, Sell_N_Volume,
        Buy_I_Value, Buy_N_Value, Sell_I_Value, Sell_N_Value
    ) VALUES (
        %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s
    )
    """

    params = [
        (
            sid,
            r.get("date"),
            ensure_numeric(r.get("Buy_CountI")),
            ensure_numeric(r.get("Buy_CountN")),
            ensure_numeric(r.get("Sell_CountI")),
            ensure_numeric(r.get("Sell_CountN")),
            ensure_numeric(r.get("Buy_I_Volume")),
            ensure_numeric(r.get("Buy_N_Volume")),
            ensure_numeric(r.get("Sell_I_Volume")),
            ensure_numeric(r.get("Sell_N_Volume")),
            ensure_numeric(r.get("Buy_I_Value")),
            ensure_numeric(r.get("Buy_N_Value")),
            ensure_numeric(r.get("Sell_I_Value")),
            ensure_numeric(r.get("Sell_N_Value")),
        )
        for r in rows
    ]

    cur = conn.cursor()
    cur.executemany(q, params)
    if BATCH_COMMIT:
        conn.commit()
    cur.close()

    logger.info("Inserted %d deals for %s", len(rows), sid)


# ---------------------------
# Save JSON
# ---------------------------
def persist_json(symbol, t, data):
    if not SAVE_JSON:
        return
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, f"{symbol}_{t}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ---------------------------
# MAIN
# ---------------------------
def main():
    symbols = load_symbols()
    if not symbols:
        logger.error("No symbols to process.")
        return

    conn = get_connection()
    if conn is None:
        logger.error("DB connection failed.")
        return

    symbol_ids = {}
    if os.path.exists(SYMBOL_IDS_FILE):
        with open(SYMBOL_IDS_FILE, "r", encoding="utf-8") as f:
            symbol_ids = json.load(f)

    try:
        for i, s in enumerate(symbols, 1):
            logger.info("Processing [%d/%d] %s", i, len(symbols), s)

            try:
                raw_p = fetch_type(s, 0)
                time.sleep(RATE_LIMIT_SECONDS)
                raw_d = fetch_type(s, 1)
                time.sleep(RATE_LIMIT_SECONDS)

                persist_json(s, 0, raw_p)
                persist_json(s, 1, raw_d)

                prices = filter_by_year(raw_p, YEAR_FILTER)
                deals = filter_by_year(raw_d, YEAR_FILTER)

                sid = symbol_ids.get(s)
                if not sid:
                    env_key = f"SYMBOL_ID_{s}"
                    if os.getenv(env_key):
                        sid = int(os.getenv(env_key))

                if not sid:
                    logger.error("No symbol_id for %s. Skipping.", s)
                    continue

                insert_prices(conn, sid, prices)
                insert_deals(conn, sid, deals)

            except Exception as e:
                logger.exception("Error in symbol %s: %s", s, e)
                continue
    finally:
        try:
            conn.close()
        except:
            pass

    logger.info("ETL finished.")


if __name__ == "__main__":
    main()
