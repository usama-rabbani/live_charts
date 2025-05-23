import pyodbc
import logging
from contextlib import contextmanager
from datetime import datetime
import streamlit as st

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load database credentials from Streamlit secrets
try:
    db_config = st.secrets["database"]
except KeyError as e:
    logger.error(f"Failed to load database credentials from secrets.toml: {e}")
    raise

@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = pyodbc.connect(
            f"DRIVER={{{db_config['DRIVER']}}};"
            f"SERVER={db_config['SERVER']};"
            f"DATABASE={db_config['DATABASE']};"
            f"UID={db_config['UID']};"
            f"PWD={db_config['PWD']};"
            f"Encrypt={db_config['ENCRYPT']};"
            f"TrustServerCertificate={db_config['TRUST_SERVER_CERTIFICATE']};"
        )
        yield conn
    except pyodbc.Error as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()

def fetch_new_execution_rows(last_seen_timestamp, var_id):
    try:
        print(f"Preparing to execute query with var_id={var_id}, last_seen_timestamp={last_seen_timestamp}")
        with get_db_connection() as conn:
            cursor = conn.cursor()
            query = """
                SELECT TOP 1 Entry_On AS timestamp, Result, Result_On
                FROM execution 
                WHERE Var_Id = ? AND Result_On > ?
                ORDER BY Result_On DESC
            """
            logger.info(f"Executing query with var_id={var_id}, last_seen_timestamp={last_seen_timestamp}")
            print("Before executing query")
            cursor.execute(query, (var_id, last_seen_timestamp))
            print("After executing query")
            rows = cursor.fetchall()
            logger.info(f"Retrieved {len(rows)} rows from database")
            print(f"Query executed successfully, number of rows: {len(rows)}")
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    except pyodbc.Error as e:
        logger.error(f"Database query error: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error in fetch_new_execution_rows: {e}")
        return []

if __name__ == "__main__":
    result = fetch_new_execution_rows(last_seen_timestamp=1, var_id=13430)  
    print("Function result:", result)
