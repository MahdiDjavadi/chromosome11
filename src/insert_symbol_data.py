import json
import os
from src.db import get_connection

SYMBOL_ID = 1
YEAR_FILTER = "1404"
SYMBOL = "فملی"


def load_json(symbol, t):
    path = f"data/{symbol}_{t}.json"
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def insert_prices(conn, symbol_id, records):
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

    cur = conn.cursor()

    batch = [
        (
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
        for r in records
        if r["date"].startswith(YEAR_FILTER)
    ]

    if batch:
        cur.executemany(query, batch)
        conn.commit()

    cur.close()


def insert_deals(conn, symbol_id, records):
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

    cur = conn.cursor()

    batch = [
        (
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
        for r in records
        if r["date"].startswith(YEAR_FILTER)
    ]

    if batch:
        cur.executemany(query, batch)
        conn.commit()

    cur.close()


if __name__ == "__main__":
    conn = get_connection()

    prices = load_json(SYMBOL, 0)
    deals = load_json(SYMBOL, 1)

    insert_prices(conn, SYMBOL_ID, prices)
    insert_deals(conn, SYMBOL_ID, deals)

    conn.close()
