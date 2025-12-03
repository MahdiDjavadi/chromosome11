# src/db.py
import os
import mysql.connector

def get_connection():
    # Read database info directly from environment (GitHub Secrets or OS env)
    host = os.getenv("MYSQL_HOST")
    port = int(os.getenv("MYSQL_PORT"))
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    database = os.getenv("MYSQL_DATABASE")
    ssl_ca = os.getenv("MYSQL_SSL_CA")

    ca_path = None
    if ssl_ca and ssl_ca.strip().startswith("-----BEGIN CERTIFICATE-----"):
        ca_path = "ca-cert.pem"
        ssl_ca = ssl_ca.replace("\\n", "\n")
        with open(ca_path, "w") as f:
            f.write(ssl_ca)

    try:
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            ssl_ca=ca_path,
            ssl_verify_cert=True if ca_path else False
        )
        print("✅ Connected to database.")
        return conn
    except mysql.connector.Error as err:
        print(f"❌ Connection failed: {err}")
        return None
