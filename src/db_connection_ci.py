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

    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            ssl_ca=ca_path,
            ssl_verify_cert=True
        )
        print("✅ Connected to database (CI).")
        return conn
    except mysql.connector.Error as err:
        print(f"❌ Connection failed (CI): {err}")
        return None


if __name__ == "__main__":
    conn = get_connection()
    if conn:
        conn.close()
