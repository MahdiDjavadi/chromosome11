# --- Local version: uses .env for testing on laptop ---

from dotenv import load_dotenv
import os
import mysql.connector

# Load environment variables from local .env file
load_dotenv()

def get_connection():
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
    MYSQL_SSL_CA = os.getenv("MYSQL_SSL_CA")

    ca_path = None
    if MYSQL_SSL_CA and MYSQL_SSL_CA.strip().startswith("-----BEGIN CERTIFICATE-----"):
        MYSQL_SSL_CA = MYSQL_SSL_CA.replace("\\n", "\n")
        ca_path = "ca-cert.pem"
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
        print("✅ Connected to database.")
        return conn
    except mysql.connector.Error as err:
        print(f"❌ Connection failed: {err}")
        return None


if __name__ == "__main__":
    conn = get_connection()
    if conn:
        conn.close()

