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
    # logger.error(f"Failed to load database credentials from secrets.toml: {e}")
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
        # logger.error(f"Database connection error: {e}")
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
                SELECT TOP 100 Entry_On AS timestamp, Result, Result_On
                FROM execution
                WHERE Var_Id = ? AND Result_On > ?
                ORDER BY Result_On DESC
            """
            # logger.info(f"Executing query with var_id={var_id}, last_seen_timestamp={last_seen_timestamp}")
            print("Before executing query")
            cursor.execute(query, (var_id, last_seen_timestamp))
            print("After executing query")
            rows = cursor.fetchall()
            # logger.info(f"Retrieved {len(rows)} rows from database")
            print(f"Query executed successfully, number of rows: {len(rows)}")
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    except pyodbc.Error as e:
        # logger.error(f"Database query error: {e}")
        return []
    except Exception as e:
        # logger.error(f"Unexpected error in fetch_new_execution_rows: {e}")
        return []

# Naya function jo VAR_OPTIONS ke liye data fetch karega
def fetch_var_options():
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            query = """
                SELECT 
                    e.Var_Id,
                    concat(u.pu_desc, ' -> ', v.Var_Desc) AS Var_Desc,
                    us.PT_Desc,
                    us.eg_id,
                    COUNT(*) AS OccurrenceCount
                FROM Execution e
                JOIN Variables v ON e.Var_Id = v.Var_Id
                JOIN Variable_Groups vg ON vg.VG_Id = v.VG_Id
                JOIN Unit u ON v.Var_Id = u.Production_Variable
                JOIN structureunit us ON us.pu_id = vg.pu_id
                WHERE 
                    u.Production_Variable <> 0 AND
                    e.Result_On >= DATEADD(DAY, -2, GETDATE())
                    AND eg_id = 'S-8656208224193'
                GROUP BY 
                    e.Var_Id, v.Var_Desc, u.pu_desc, us.PT_Desc, us.eg_id
                ORDER BY 
                    OccurrenceCount DESC
                OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            # logger.info(f"Retrieved {len(rows)} rows for VAR_OPTIONS")
            print(f"Fetched {len(rows)} rows for VAR_OPTIONS")
            # Sirf Var_Id aur Var_Desc return karenge jo dropdown ke liye zaroori hai
            return [{"Var_Id": row[0], "Var_Desc": row[1]} for row in rows]
    except pyodbc.Error as e:
        # logger.error(f"Database query error in fetch_var_options: {e}")
        return []
    except Exception as e:
        # logger.error(f"Unexpected error in fetch_var_options: {e}")
        return []

if __name__ == "__main__":
    result = fetch_new_execution_rows(last_seen_timestamp=1, var_id=13430)
    print("Function result:", result)