import pandas as pd
import sqlalchemy
from datetime import datetime
from dotenv import load_dotenv
import os

from utils.db_conn import db_connection
from utils.log import etl_log, read_etl_log


temp_file = os.getenv("TEMP_FILE")
dir_excel = os.getenv("DIR_EXCEL")


def extract_database(table_name: str): 
    try:
        
        # create connection to database
        _,_ ,stg_engine= db_connection()

        # Get date from previous process
        filter_log = {"step_name": "warehouse",
                    "table_name": table_name,
                    "status": "success",
                    "process": "load"}
        etl_date = read_etl_log(filter_log)

        if(etl_date['max'][0] == None):
            etl_date = '1111-01-01'
        else:
            etl_date = etl_date[max][0]

        
        query = f"SELECT * FROM {table_name} WHERE created_at  > %s::timestamp"

        #Execute the query with pd.read_sql
        df = pd.read_sql(sql=query, con=stg_engine, params=(etl_date,))

        df.to_csv(f"{temp_file}/{table_name}.csv", index=False)
        
        log_msg = {
                "step" : "warehouse",
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
            "step" : "warehouse",
            "process":"extraction",
            "status": "failed",
            "source": "database",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
            "error_msg": str(e)
        }
    finally:
        etl_log(log_msg)