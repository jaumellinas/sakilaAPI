from contextlib import contextmanager
import pymysql
from fastapi import HTTPException
from app.core.config import DB_CONFIG

pymysql.install_as_MySQLdb()

@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        yield conn
    except pymysql.Error as e:
        raise HTTPException(
            status_code=500, detail=f"Error de base de datos: {str(e)}"
        )
    finally:
        if conn:
            conn.close()