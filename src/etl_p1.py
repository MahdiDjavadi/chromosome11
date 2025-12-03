# src/etl.py (ULTRA OPTIMIZED)

import os, json, time, logging, requests, sys
from datetime import datetime
from urllib.parse import quote
import backoff

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CURRENT_DIR)
sys.path.insert(0, ROOT_DIR)
from src.db import get_connection

SYMBOLS_FILE = os.getenv("SYMBOLS_FILE", os.path.join(ROOT_DIR, "data", "symbols.txt"))
SYMBOL_IDS_FILE = os.path.join(ROOT_DIR, "data", "symbol_ids.json")
API_KEY = os.getenv("BRSAPI_API_KEY")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}

BASE_URL = "https://brsapi.ir/Api/Tsetmc/History.php"
YEAR = "1404"
MONTHS = {"06","07","08","09","10","11","12"}
RATE = 0.20

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("etl")

def load_symbols():
    with open(SYMBOLS_FILE, "r", encoding="utf-8") as f:
        return [s.strip() for s in f if s.strip()]

def ensure_symbol_ids(symbols):
    if os.path.exists(SYMBOL_IDS_FILE):
        return json.load(open(SYMBOL_IDS_FILE, encoding="utf-8"))
    m = {s:i+1 for i,s in enumerate(symbols)}
    json.dump(m, open(SYMBOL_IDS_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    return m

@backoff.on_exception(backoff.expo, (requests.exceptions.RequestException,))
def fetch(symbol, t):
    url = f"{BASE_URL}?key={API_KEY}&type={t}&l18={quote(symbol)}"
    r = requests.get(url, headers=HEADERS, timeout=15)
    if r.status_code == 400:
        raise SystemExit(400)
    r.raise_for_status()
    d = r.json()
    return d if isinstance(d, list) else []

def filt(rows):
    out = []
    for r in rows:
        d = r.get("date","")
        if len(d)>=7 and d[:4]==YEAR and d[5:7] in MONTHS:
            out.append(r)
    return out

def num(v, cast=int):
    try: return cast(v)
    except: return None

PRICE_Q = """
REPLACE INTO symbol_price (
symbol_id,date,time,tno,tvol,tval,pmin,pmax,py,pf,pl,plc,plp,pc,pcc,pcp
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

DEAL_Q = """
REPLACE INTO symbol_deals (
symbol_id,date,
Buy_CountI,Buy_CountN,Sell_CountI,Sell_CountN,
Buy_I_Volume,Buy_N_Volume,Sell_I_Volume,Sell_N_Volume,
Buy_I_Value,Buy_N_Value,Sell_I_Value,Sell_N_Value
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

def main():
    symbols = load_symbols()
    ids = ensure_symbol_ids(symbols)
    conn = get_connection()
    cur = conn.cursor()

    for i,s in enumerate(symbols,1):
        log.info(f"{i}/{len(symbols)} - {s}")
        sid = ids[s]

        try:
            raw0 = fetch(s,0); time.sleep(RATE)
            raw1 = fetch(s,1); time.sleep(RATE)
        except SystemExit:
            log.error(f"400 skipped: {s}")
            continue
        except Exception as e:
            log.error(f"{s}: {e}")
            continue

        p = filt(raw0)
        d = filt(raw1)

        if p:
            rows = [
                (sid,r["date"],r.get("time"),
                 num(r.get("tno")),num(r.get("tvol")),num(r.get("tval")),
                 num(r.get("pmin")),num(r.get("pmax")),num(r.get("py")),
                 num(r.get("pf")),num(r.get("pl")),num(r.get("plc")),
                 num(r.get("plp"),float),num(r.get("pc")),num(r.get("pcc")),
                 num(r.get("pcp"),float))
                for r in p
            ]
            cur.executemany(PRICE_Q, rows)

        if d:
            rows = [
                (sid,r["date"],
                 num(r.get("Buy_CountI")),num(r.get("Buy_CountN")),
                 num(r.get("Sell_CountI")),num(r.get("Sell_CountN")),
                 num(r.get("Buy_I_Volume")),num(r.get("Buy_N_Volume")),
                 num(r.get("Sell_I_Volume")),num(r.get("Sell_N_Volume")),
                 num(r.get("Buy_I_Value")),num(r.get("Buy_N_Value")),
                 num(r.get("Sell_I_Value")),num(r.get("Sell_N_Value")))
                for r in d
            ]
            cur.executemany(DEAL_Q, rows)

        conn.commit()

    cur.close()
    conn.close()
    log.info("DONE")

if __name__ == "__main__":
    main()
