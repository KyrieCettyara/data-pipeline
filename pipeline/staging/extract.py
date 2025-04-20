import pandas as pd
import sqlalchemy
from datetime import datetime
from dotenv import load_dotenv
import os

from utils.db_conn import db_connection
from utils.log import etl_log, read_etl_log


temp_file = os.getenv("TEMP_FILE")
dir_excel = os.getenv("DIR_EXCEL")

def extract_list_table(db_name):
    try:
        # Define db connection engine
        src_engine,_ ,_= db_connection()
        
        # Get list of tables in the database
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        df = pd.read_sql(sql=query, con=src_engine)
        return df
    except Exception as e:
        print(e)
        return None

def extract_database(db_name:str, table_name: str): 
    try:
        
        # create connection to database
        src_engine,_,_ = db_connection()

        # Get date from previous process
        filter_log = {"step_name": "staging",
                    "table_name": table_name,
                    "status": "success",
                    "process": "load"}
        etl_date = read_etl_log(filter_log)


        # If no previous extraction has been recorded (etl_date is empty), set etl_date to '1111-01-01' indicating the initial load.
        # Otherwise, retrieve data added since the last successful extraction (etl_date).
        if(etl_date['max'][0] == None):
            etl_date = '1111-01-01'
        else:
            etl_date = etl_date[max][0]

        
        query = f"SELECT * FROM {table_name} WHERE created_at  > %s::timestamp"

        #Execute the query with pd.read_sql
        df = pd.read_sql(sql=query, con=src_engine, params=(etl_date,))
        df.to_csv(f"{temp_file}/{table_name}.csv", index=False)
        log_msg = {
                "step" : "staging",
                "process":"extraction",
                "status": "success",
                "source": "database",
                "table_name": table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        return log_msg
    except Exception as e:
        print(e)
        log_msg = {
            "step" : "staging",
            "process":"extraction",
            "status": "failed",
            "source": "database",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
            "error_msg": str(e)
        }
    finally:
        etl_log(log_msg)

def extract_sheet(file_name: str): 
    try:
        
        # read from file
        df_result = pd.read_csv(f"{dir_excel}/{file_name}.csv")
        df_result.to_csv(f"{temp_file}{file_name}.csv", index=False)

        log_msg = {
                "step" : "staging",
                "process":"extraction",
                "status": "success",
                "source": "excel",
                "table_name": file_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        return log_msg
    except Exception as e:
        print(e)
        log_msg = {
            "step" : "staging",
            "process":"extraction",
            "status": "failed",
            "source": "excel",
            "table_name": file_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
            "error_msg": str(e)
        }
    finally:
        etl_log(log_msg)