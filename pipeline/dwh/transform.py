import pandas as pd
from datetime import datetime
from utils.log import etl_log
from dotenv import load_dotenv
import os

def transform_commpany(table_name: str) -> pd.DataFrame:
    try:
        temp_file = os.getenv("TEMP_FILE")
        process = "transformation"

        df = pd.read_csv(f"{temp_file}/{table_name}.csv")

        df = df.rename(columns={'office_id': 'office_nk'})  
        
        df = df.drop_duplicates(subset='office_nk')
    
        # drop unnecessary columns
        df = df.drop(columns=['address1'
                                  , 'address2'
                                  , 'city'
                                  , 'zip_code'
                                  , 'state_code'
                                  , 'country_code'
                                  , 'latitude'
                                  , 'longitude'
                                  , 'updated_at'])

        log_msg = {
                "step" : "warehouse",
                "process": process,
                "status": "success",
                "source": "staging",
                "table_name": table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                }
        
        return df
    except Exception as e:
        print(e)
        log_msg = {
            "step" : "warehouse",
            "process": process,
            "status": "failed",
            "source": "staging",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp,
            "error_msg": str(e)
            }
    finally:
        # Save the log message
        etl_log(log_msg)

def transform_funding_rounds(table_name: str) -> pd.DataFrame:
    try:
        temp_file = os.getenv("TEMP_FILE")
        process = "transformation"

        df = pd.read_csv(f"{temp_file}/{table_name}.csv")

        df = df.rename(columns={'founding_round_id': 'founding_round_nk'})  
        
        df = df.drop_duplicates(subset='office_nk')
    
        # drop unnecessary columns
        df = df.drop(columns=['funding_round_type'
                              , 'funding_round_code'
                              , 'updated_at'
                              , 'participants'
                              , 'created_by'
                              , 'updated_at'])

        log_msg = {
                "step" : "warehouse",
                "process": process,
                "status": "success",
                "source": "staging",
                "table_name": table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                }
        
        return df
    except Exception as e:
        print(e)
        log_msg = {
            "step" : "warehouse",
            "process": process,
            "status": "failed",
            "source": "staging",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp,
            "error_msg": str(e)
            }
    finally:
        # Save the log message
        etl_log(log_msg)