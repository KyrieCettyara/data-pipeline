import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
import os
from pangres import upsert
from datetime import datetime

from utils.db_conn import db_connection, read_sql
from utils.log import etl_log
from sqlalchemy import text,inspect

temp_file = os.getenv("TEMP_FILE")


def load_staging(table_name: str, idx_name:str, source):
    try:
        # create connection to database
        _,_,stg_engine = db_connection()

        df = pd.read_csv(f"{temp_file}/{table_name}.csv")        
        df = df.set_index(idx_name)


        # Do upsert (Update for existing data and Insert for new data)
        upsert(con = stg_engine,
                df = df,
                table_name = table_name,
                schema = "public",
                if_row_exists = "update")

        # Delete temporary file
        os.remove(f"{temp_file}/{table_name}.csv")
            
        #create success log message
        log_msg = {
                "step" : "staging",
                "process":"load",
                "status": "success",
                "source": source,
                "table_name": table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        return log_msg
    except Exception as e:
        #create fail log message
        log_msg = {
            "step" : "staging",
            "process":"load",
            "status": "failed",
            "source": source,
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S") , # Current timestamp
            "error_msg": str(e)
        }
        print(e)       
    finally:
        etl_log(log_msg)

    