import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
import os
from utils.db_conn import read_sql, db_connection


load_dotenv(".env")

MODEL_PATH_LOG_ETL = os.getenv("MODEL_PATH_LOG_ETL")

def etl_log(log_msg: dict):
    try:
        # create connection to database
        _,conn,_ = db_connection()
        
        # convert dictionary to dataframe
        df_log = pd.DataFrame([log_msg])

        #extract data log
        df_log.to_sql(name = "etl_log",  # Your log table
                        con = conn,
                        if_exists = "append",
                        index = False)
    except Exception as e:
        print("Can't save your log message. Cause: ", str(e))


def read_etl_log(filter_params: dict):
    try:
        # create connection to database
        _,conn,_ = db_connection()
        
        # To help with the incremental process, get the etl_date from the relevant process
        query = sqlalchemy.text(read_sql(MODEL_PATH_LOG_ETL,"log"))

        # Execute the query with pd.read_sql
        df = pd.read_sql(sql=query, con=conn, params=(filter_params,))

        #return extracted data
        return df
    except Exception as e:
        print("Can't execute your query. Cause: ", str(e))