from db import get_connection

def execute(query, params=None):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(query, params or ())
        conn.commit()
    finally:
        cur.close()
        conn.close()

def upsert(query, params=None):
    execute(query, params)

def fetch_all(query, params=None):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(query, params or ())
        rows = cur.fetchall()
        return rows
    finally:
        cur.close()
        conn.close()
