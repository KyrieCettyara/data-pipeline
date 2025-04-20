import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
import os
from pangres import upsert
from datetime import datetime

from utils.db_conn import dwh_connection
from utils.log import etl_log
from sqlalchemy import text,inspect

temp_file = os.getenv("TEMP_FILE")


def load_warehouse(data, table_name: str, idx_name:str, source, table_process:str):
    try:
        # create connection to database
        dwh_engine = dwh_connection()
        
        # set data index or primary key
        data = data.set_index(idx_name)
        
        # Do upsert (Update for existing data and Insert for new data)
        upsert(con = dwh_engine,
                df = data,
                table_name = table_name,
                schema = "public",
                if_row_exists = "update")
        
        #create success log message
        log_msg = {
                "step" : "warehouse",
                "process":"load",
                "status": "success",
                "source": source,
                "table_name": table_process,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        # Delete temporary file
        os.remove(f"{temp_file}/{table_name}.csv")
        # return data
    except Exception as e:

        #create fail log message
        log_msg = {
            "step" : "warehouse",
            "process":"load",
            "status": "failed",
            "source": source,
            "table_name": table_process,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S") , # Current timestamp
            "error_msg": str(e)
        }
        print(e)
    finally:
        etl_log(log_msg)
    