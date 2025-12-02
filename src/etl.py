# src/etl.py
import os
import json
import time
import logging
from typing import List, Dict, Any
from urllib.parse import quote

import backoff
import requests
from dotenv import load_dotenv

from src.db import get_connection

load_dotenv()

# CONFIG (via env or defaults)
API_KEY = os.getenv("BRSAPI_API_KEY")
HEADERS = {
    "User-Agent": os.getenv(
        "ETL_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}
BASE_URL = "https://brsapi.ir/Api/Tsetmc/History.php"
YEAR_FILTER = os.getenv("YEAR_FILTER", "1404")
SAVE_JSON = os.getenv("SAVE_JSON", "true").lower() in ("1", "true", "yes")
RATE_LIMIT_SECONDS = float(os.getenv("RATE_LIMIT_SECONDS", "0.30"))  # delay between requests
CURL_FALLBACK = os.getenv("CURL_FALLBACK", "true").lower() in ("1", "true", "yes")
BATCH_COMMIT_PER_SYMBOL = os.getenv("BATCH_COMMIT_PER_SYMBOL", "true").lower() in ("1", "true", "yes")
SYMBOLS_FILE = os.getenv("SYMBOLS_FILE", "symbols.txt")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# logging
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("tsetmc-etl")


def load_symbols() -> List[str]:
    env_list = os.getenv("SYMBOLS")
    if env_list:
        return [s.strip() for s in env_list.split(",") if s.strip()]
    if os.path.exists(SYMBOLS_FILE):
        with open(SYMBOLS_FILE, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    return []


@backoff.on_exception(backoff.expo, (requests.exceptions.RequestException,), max_tries=5)
def fetch_type(symbol: str, t: int) -> List[Dict[str, Any]]:
    symbol_enc = quote(symbol)
    curl_flag = "&curl=1" if CURL_FALLBACK else ""
    url = f"{BASE_URL}?key={API_KEY}&type={t}{curl_flag}&l18={symbol_enc}"
    logger.debug("Request URL: %s", url)
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        # Defensive: if API returned a dict with wrapper
        # try to locate list inside
        for v in data.values() if isinstance(data, dict) else []:
            if isinstance(v, list):
                return v
        return []
    return data


def filter_by_year(records: List[Dict[str, Any]], year_prefix: str) -> List[Dict[str, Any]]:
    return [r for r in records if isinstance(r.get("date"), str) and r["date"].startswith(year_prefix)]


def ensure_numeric(v, cast_type=int, default=None):
    try:
        return cast_type(v) if v is not None and v != "" else default
    except Exception:
        try:
            return cast_type(float(v))
        except Exception:
            return default


def insert_prices(conn, symbol_id: int, records: List[Dict[str, Any]]):
    if not records:
        logger.debug("No price records for symbol_id=%s", symbol_id)
        return
    query = """
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
    params = []
    for r in records:
        params.append(
            (
                symbol_id,
                r.get("date"),
                r.get("time"),
                ensure_numeric(r.get("tno"), int, None),
                ensure_numeric(r.get("tvol"), int, None),
                ensure_numeric(r.get("tval"), int, None),
                ensure_numeric(r.get("pmin"), int, None),
                ensure_numeric(r.get("pmax"), int, None),
                ensure_numeric(r.get("py"), int, None),
                ensure_numeric(r.get("pf"), int, None),
                ensure_numeric(r.get("pl"), int, None),
                ensure_numeric(r.get("plc"), int, None),
                ensure_numeric(r.get("plp"), float, None),
                ensure_numeric(r.get("pc"), int, None),
                ensure_numeric(r.get("pcc"), int, None),
                ensure_numeric(r.get("pcp"), float, None),
            )
        )
    cur = conn.cursor()
    cur.executemany(query, params)
    if BATCH_COMMIT_PER_SYMBOL:
        conn.commit()
    cur.close()
    logger.info("Inserted %d price rows for symbol_id=%s", len(params), symbol_id)


def insert_deals(conn, symbol_id: int, records: List[Dict[str, Any]]):
    if not records:
        logger.debug("No deal records for symbol_id=%s", symbol_id)
        return
    query = """
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
    params = []
    for r in records:
        params.append(
            (
                symbol_id,
                r.get("date"),
                ensure_numeric(r.get("Buy_CountI"), int, None),
                ensure_numeric(r.get("Buy_CountN"), int, None),
                ensure_numeric(r.get("Sell_CountI"), int, None),
                ensure_numeric(r.get("Sell_CountN"), int, None),
                ensure_numeric(r.get("Buy_I_Volume"), int, None),
                ensure_numeric(r.get("Buy_N_Volume"), int, None),
                ensure_numeric(r.get("Sell_I_Volume"), int, None),
                ensure_numeric(r.get("Sell_N_Volume"), int, None),
                ensure_numeric(r.get("Buy_I_Value"), int, None),
                ensure_numeric(r.get("Buy_N_Value"), int, None),
                ensure_numeric(r.get("Sell_I_Value"), int, None),
                ensure_numeric(r.get("Sell_N_Value"), int, None),
            )
        )
    cur = conn.cursor()
    cur.executemany(query, params)
    if BATCH_COMMIT_PER_SYMBOL:
        conn.commit()
    cur.close()
    logger.info("Inserted %d deal rows for symbol_id=%s", len(params), symbol_id)


def persist_json(symbol: str, t: int, data):
    if not SAVE_JSON:
        return
    os.makedirs("data", exist_ok=True)
    path = os.path.join("data", f"{symbol}_{t}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    logger.debug("Saved JSON %s", path)


def main():
    symbols = load_symbols()
    if not symbols:
        logger.error("No symbols provided. Create %s or set SYMBOLS env var.", SYMBOLS_FILE)
        return

    conn = get_connection()
    if conn is None:
        logger.error("DB connection failed, aborting.")
        return

    try:
        for idx, symbol in enumerate(symbols, start=1):
            logger.info("Processing [%d/%d] %s", idx, len(symbols), symbol)
            try:
                # fetch both types
                prices_raw = fetch_type(symbol, 0)
                time.sleep(RATE_LIMIT_SECONDS)
                deals_raw = fetch_type(symbol, 1)
                time.sleep(RATE_LIMIT_SECONDS)

                # persist raw JSON if desired
                persist_json(symbol, 0, prices_raw)
                persist_json(symbol, 1, deals_raw)

                # filter by year
                prices = filter_by_year(prices_raw, YEAR_FILTER)
                deals = filter_by_year(deals_raw, YEAR_FILTER)

                # map symbol -> symbol_id (user manages symbols table)
                # Here we assume you have a mapping function or table; for now use provided symbol id env or file mapping
                # If SYMBOL_IDS mapping exists as JSON file symbol_ids.json, read it; else expect env SYMBOL_ID_<TICKER>
                symbol_id = None
                # try env override first
                env_key = f"SYMBOL_ID_{symbol}"
                if os.getenv(env_key):
                    symbol_id = int(os.getenv(env_key))
                else:
                    # try a simple mapping file
                    if os.path.exists("symbol_ids.json"):
                        with open("symbol_ids.json", "r", encoding="utf-8") as f:
                            mp = json.load(f)
                            symbol_id = int(mp.get(symbol)) if mp.get(symbol) else None

                if symbol_id is None:
                    # fallback: require SYMBOL_IDS file, else skip
                    logger.error("No symbol_id found for %s. Provide via env %s or symbol_ids.json", symbol, env_key)
                    continue

                # insert using single connection, executemany for batches
                insert_prices(conn, symbol_id, prices)
                insert_deals(conn, symbol_id, deals)

            except Exception as e:
                logger.exception("Failed processing symbol %s: %s", symbol, e)
                # continue with next symbol
                continue
    finally:
        try:
            conn.close()
        except Exception:
            pass
    logger.info("ETL run finished.")


if __name__ == "__main__":
    main()
