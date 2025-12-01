import os
import mysql.connector

def get_connection():
    host = os.getenv("MYSQL_HOST")
    port = int(os.getenv("MYSQL_PORT", 3306))
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    database = os.getenv("MYSQL_DATABASE")
    ssl_ca = os.getenv("MYSQL_SSL_CA")

    ca_path = None
    if ssl_ca and ssl_ca.strip().startswith("-----BEGIN CERTIFICATE-----"):
        ca_path = "/tmp/ca-cert.pem"
        with open(ca_path, "w") as f:
            f.write(ssl_ca)

    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        ssl_ca=ca_path,
        ssl_verify_cert=True
    )
