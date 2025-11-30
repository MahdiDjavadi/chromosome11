import os
import mysql.connector

def get_connection():
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
    MYSQL_SSL_CA = os.getenv("MYSQL_SSL_CA")

    ca_path = None
    if MYSQL_SSL_CA and MYSQL_SSL_CA.strip().startswith("-----BEGIN CERTIFICATE-----"):
        ca_path = "/tmp/ca-cert.pem"
        with open(ca_path, "w") as f:
            f.write(MYSQL_SSL_CA)

    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        ssl_ca=ca_path,
        ssl_verify_cert=True
    )

def upsert(query, params):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    cur.close()
    conn.close()
