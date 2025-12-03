import os
import mysql.connector

def get_connection_from_env(env):
    host = env["MYSQL_HOST"]
    port = int(env.get("MYSQL_PORT", 3306))
    user = env["MYSQL_USER"]
    password = env["MYSQL_PASSWORD"]
    database = env["MYSQL_DATABASE"]
    ssl_ca = env.get("MYSQL_SSL_CA")

    ca_path = None
    if ssl_ca and ssl_ca.strip().startswith("-----BEGIN CERTIFICATE-----"):
        ca_path = "ca-cert.pem"
        ssl_ca = ssl_ca.replace("\\n", "\n")
        with open(ca_path, "w", encoding="utf-8") as f:
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
