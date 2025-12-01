from db import get_connection

def fetch_all(query, params=None):
    conn = get_connection()
    if not conn:
        return []
    cur = conn.cursor(dictionary=True)
    cur.execute(query, params or ())
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def upsert(query, params):
    conn = get_connection()
    if not conn:
        return False
    cur = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    cur.close()
    conn.close()
    return True
