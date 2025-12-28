import os
from dotenv import load_dotenv
import pymysql

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("MARIADB_HOST"),
    "port": int(os.getenv("MARIADB_PORT")),
    "user": os.getenv("MARIADB_USER"),
    "password": os.getenv("MARIADB_PASSWORD"),
    "database": os.getenv("MARIADB_DATABASE"),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.Cursor
}

SECRET_KEY = os.getenv("SECRET_KEY",)
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1440