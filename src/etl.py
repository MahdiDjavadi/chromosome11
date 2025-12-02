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

# Load .env
load_dotenv()

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SYMBOLS_FILE = os.getenv("SYMBOLS_FILE", os.path.join(BASE_DIR, "data", "symbols.txt"))
SYMBOL_IDS_FILE = os.path.join(BASE_DIR, "data", "symbol_ids.json")

# Config
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
RATE_LIMIT_SECONDS = float(os.getenv("RATE_LIMIT_SECONDS", "0.30"))
CURL_FALLBACK = os.getenv("CURL_FALLBACK", "true").lower() in ("1", "true", "yes")
BATCH_COMMIT_PER_SYMBOL = os.getenv("BATCH_COMMIT_PER_SYMBOL", "true").lower() in ("1", "true", "yes")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Logging
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("tsetmc-etl")


def load_symbols() -> List[str]:
    env_list = os.getenv("SYMBOLS")
    if env_list:
        return [s.strip() for s in env_list.split(",") if s.strip()]
    if os.path.exists(SYMBOLS_FILE):
        with open(SYMBOLS_FILE, "r", encoding="utf-8") as f:
            symbols = [line.strip() for line in f if line.strip()]
            return symbols
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
        )
    cur = conn.cursor()
    cur.executemany(query, params)
    if BATCH_COMMIT_PER_SYMBOL:
        conn.commit()
    cur.close()
    logger.info("Inserted %d price rows for symbol_id=%s", len(params), symbol_id)


def insert_deals(conn, symbol_id: int, records: List[Dict[str, Any]]):
    if not records:
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
    os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)
    path = os.path.join(BASE_DIR, "data", f"{symbol}_{t}.json")
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

    symbol_ids_mapping = {}
    if os.path.exists(SYMBOL_IDS_FILE):
        with open(SYMBOL_IDS_FILE, "r", encoding="utf-8") as f:
            symbol_ids_mapping = json.load(f)

    try:
        for idx, symbol in enumerate(symbols, start=1):
            logger.info("Processing [%d/%d] %s", idx, len(symbols), symbol)
            try:
                prices_raw = fetch_type(symbol, 0)
                time.sleep(RATE_LIMIT_SECONDS)
                deals_raw = fetch_type(symbol, 1)
                time.sleep(RATE_LIMIT_SECONDS)

                persist_json(symbol, 0, prices_raw)
                persist_json(symbol, 1, deals_raw)

                prices = filter_by_year(prices_raw, YEAR_FILTER)
                deals = filter_by_year(deals_raw, YEAR_FILTER)

                symbol_id = symbol_ids_mapping.get(symbol)
                if not symbol_id:
                    env_key = f"SYMBOL_ID_{symbol}"
                    if os.getenv(env_key):
                        symbol_id = int(os.getenv(env_key))
                if not symbol_id:
                    logger.error("No symbol_id found for %s. Skip.", symbol)
                    continue

                insert_prices(conn, symbol_id, prices)
                insert_deals(conn, symbol_id, deals)

            except Exception as e:
                logger.exception("Failed processing symbol %s: %s", symbol, e)
                continue
    finally:
        try:
            conn.close()
        except Exception:
            pass
    logger.info("ETL run finished.")


if __name__ == "__main__":
    main()
