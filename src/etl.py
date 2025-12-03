# src/etl.py

import os
import json
import time
import logging
from typing import List, Dict, Any
from urllib.parse import quote

import backoff
import requests

# --- Force CI to recognize "src" as package ---
import sys
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CURRENT_DIR)
sys.path.insert(0, ROOT_DIR)

from src.db import get_connection

# Paths
SYMBOLS_FILE = os.getenv("SYMBOLS_FILE", os.path.join(ROOT_DIR, "data", "symbols.txt"))
SYMBOL_IDS_FILE = os.path.join(ROOT_DIR, "data", "symbol_ids.json")

API_KEY = os.getenv("BRSAPI_API_KEY")
HEADERS = {
    "User-Agent": os.getenv(
        "ETL_USER_AGENT",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}

BASE_URL = "https://brsapi.ir/Api/Tsetmc/History.php"
YEAR_FILTER = os.getenv("YEAR_FILTER", "1404")
SAVE_JSON = os.getenv("SAVE_JSON", "false").lower() in ("1", "true")
RATE_LIMIT_SECONDS = 0.4
BATCH_COMMIT = True

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("etl")


def load_symbols() -> List[str]:
    if os.getenv("SYMBOLS"):
        return [s.strip() for s in os.getenv("SYMBOLS").split(",") if s.strip()]
    if os.path.exists(SYMBOLS_FILE):
        with open(SYMBOLS_FILE, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    return []


@backoff.on_exception(backoff.expo, (requests.exceptions.RequestException,), max_tries=5)
def fetch_type(symbol: str, t: int):
    url = f"{BASE_URL}?key={API_KEY}&type={t}&l18={quote(symbol)}"
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else []


def filter_by_year(rows, year):
    return [r for r in rows if isinstance(r.get("date"), str) and r["date"].startswith(year)]


def num(v, cast=int):
    try:
        return cast(v)
    except:
        return None


def insert_prices(conn, sid, rows):
    if not rows:
        return
    q = """
    REPLACE INTO symbol_price (
      symbol_id,date,time,tno,tvol,tval,pmin,pmax,py,pf,pl,plc,plp,pc,pcc,pcp
    ) VALUES (
      %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
    )
    """
    p = [(sid, r.get("date"), r.get("time"),
          num(r.get("tno")), num(r.get("tvol")), num(r.get("tval")),
          num(r.get("pmin")), num(r.get("pmax")), num(r.get("py")),
          num(r.get("pf")), num(r.get("pl")), num(r.get("plc")),
          num(r.get("plp"), float), num(r.get("pc")), num(r.get("pcc")),
          num(r.get("pcp"), float)) for r in rows]

    cur = conn.cursor()
    cur.executemany(q, p)
    if BATCH_COMMIT:
        conn.commit()
    cur.close()


def insert_deals(conn, sid, rows):
    if not rows:
        return
    q = """
    REPLACE INTO symbol_deals (
      symbol_id,date,
      Buy_CountI,Buy_CountN,Sell_CountI,Sell_CountN,
      Buy_I_Volume,Buy_N_Volume,Sell_I_Volume,Sell_N_Volume,
      Buy_I_Value,Buy_N_Value,Sell_I_Value,Sell_N_Value
    ) VALUES (
      %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
    )
    """
    p = [(sid, r.get("date"),
          num(r.get("Buy_CountI")), num(r.get("Buy_CountN")),
          num(r.get("Sell_CountI")), num(r.get("Sell_CountN")),
          num(r.get("Buy_I_Volume")), num(r.get("Buy_N_Volume")),
          num(r.get("Sell_I_Volume")), num(r.get("Sell_N_Volume")),
          num(r.get("Buy_I_Value")), num(r.get("Buy_N_Value")),
          num(r.get("Sell_I_Value")), num(r.get("Sell_N_Value"))) for r in rows]

    cur = conn.cursor()
    cur.executemany(q, p)
    if BATCH_COMMIT:
        conn.commit()
    cur.close()


def save_json(symbol, t, data):
    if not SAVE_JSON:
        return
    d = os.path.join(ROOT_DIR, "data")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, f"{symbol}_{t}.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    symbols = load_symbols()
    if not symbols:
        logger.error("No symbols found")
        return

    conn = get_connection()
    if not conn:
        logger.error("DB connection failed")
        return

    mapping = {}
    if os.path.exists(SYMBOL_IDS_FILE):
        with open(SYMBOL_IDS_FILE, "r", encoding="utf-8") as f:
            mapping = json.load(f)

    for i, sym in enumerate(symbols, 1):
        logger.info(f"Processing {i}/{len(symbols)} - {sym}")

        try:
            raw0 = fetch_type(sym, 0)
            time.sleep(RATE_LIMIT_SECONDS)

            raw1 = fetch_type(sym, 1)
            time.sleep(RATE_LIMIT_SECONDS)

            save_json(sym, 0, raw0)
            save_json(sym, 1, raw1)

            prices = filter_by_year(raw0, YEAR_FILTER)
            deals = filter_by_year(raw1, YEAR_FILTER)

            sid = mapping.get(sym)
            if not sid:
                env_id = os.getenv(f"SYMBOL_ID_{sym}")
                sid = int(env_id) if env_id else None
            if not sid:
                logger.error(f"No symbol_id for {sym}")
                continue

            insert_prices(conn, sid, prices)
            insert_deals(conn, sid, deals)

        except Exception as e:
            logger.error(f"Error symbol={sym}: {e}")

    try:
        conn.close()
    except:
        pass

    logger.info("ETL finished.")
    

if __name__ == "__main__":
    main()
