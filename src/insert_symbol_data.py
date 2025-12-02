import json
import os
from utils_db import upsert

SYMBOL_ID = 1  # فملی

def load_json(symbol, t):
    path = f"data/{symbol}_{t}.json"
    if not os.path.exists(path):
        print(f"❌ File not found: {path}")
        return None
    
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def insert_prices(symbol_id, records):
    query = """
    REPLACE INTO symbol_price (
        symbol_id, date, time, tno, tvol, tval,
        pmin, pmax, py, pf, pl, plc, plp,
        pc, pcc, pcp
    ) VALUES (
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s
    );
    """
    for r in records:
        params = (
            symbol_id,
            r["date"],
            r["time"],
            r["tno"],
            r["tvol"],
            r["tval"],
            r["pmin"],
            r["pmax"],
            r["py"],
            r["pf"],
            r["pl"],
            r["plc"],
            r["plp"],
            r["pc"],
            r["pcc"],
            r["pcp"],
        )
        upsert(query, params)
    print("✔️ Prices inserted.")


def insert_deals(symbol_id, records):
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
    );
    """
    for r in records:
        params = (
            symbol_id,
            r["date"],
            r["Buy_CountI"],
            r["Buy_CountN"],
            r["Sell_CountI"],
            r["Sell_CountN"],
            r["Buy_I_Volume"],
            r["Buy_N_Volume"],
            r["Sell_I_Volume"],
            r["Sell_N_Volume"],
            r["Buy_I_Value"],
            r["Buy_N_Value"],
            r["Sell_I_Value"],
            r["Sell_N_Value"],
        )
        upsert(query, params)
    print("✔️ Deals inserted.")


if __name__ == "__main__":
    symbol = "فملی"

    prices = load_json(symbol, 0)
    deals = load_json(symbol, 1)

    if prices:
        insert_prices(SYMBOL_ID, prices)

    if deals:
        insert_deals(SYMBOL_ID, deals)

    print("🎉 All done for فملی!")
