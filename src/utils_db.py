def fetch_all(conn, query, params=None):
    cur = conn.cursor(dictionary=True)
    cur.execute(query, params or ())
    rows = cur.fetchall()
    cur.close()
    return rows

def upsert(conn, query, params):
    cur = conn.cursor()
    cur.executemany(query, params)
    conn.commit()
    cur.close()
    return True
