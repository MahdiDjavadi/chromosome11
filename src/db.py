import os
import tempfile
import mysql.connector

def _get_env(*names, default=None):
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return default

def get_connection():
    host = _get_env("MYSQL_HOST", "DB_HOST")
    port = _get_env("MYSQL_PORT", "DB_PORT", default="3306")
    user = _get_env("MYSQL_USER", "DB_USER")
    password = _get_env("MYSQL_PASSWORD", "DB_PASS", "DB_PASSWORD")
    database = _get_env("MYSQL_DATABASE", "DB_NAME")
    ssl_ca = _get_env("MYSQL_SSL_CA", "CA_SSL_KEY", "MYSQL_SSL_CA", "DB_SSL_CA")

    # Normalize port to int
    try:
        port = int(port)
    except Exception:
        port = 3306

    ca_path = None
    if ssl_ca:
        # handle escaped newlines
        ssl_ca_str = ssl_ca.replace("\\n", "\n")
        tmpdir = tempfile.gettempdir()
        ca_path = os.path.join(tmpdir, "ca-cert.pem")
        with open(ca_path, "w", encoding="utf-8") as f:
            f.write(ssl_ca_str)

    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        ssl_ca=ca_path,
        ssl_verify_cert=True if ca_path else False
    )
    return conn
