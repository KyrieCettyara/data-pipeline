from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')
from dotenv import load_dotenv
import os

def db_connection():
    try:
        src_database = os.getenv("SRC_POSTGRES_DB")
        src_host = os.getenv("SRC_POSTGRES_HOST")
        src_user = os.getenv("SRC_POSTGRES_USER")
        src_password = os.getenv("SRC_POSTGRES_PASSWORD")
        src_port = os.getenv("SRC_POSTGRES_PORT")

        log_database = os.getenv("LOG_POSTGRES_DB")
        log_host = os.getenv("LOG_POSTGRES_HOST")
        log_user = os.getenv("LOG_POSTGRES_USER")
        log_password = os.getenv("LOG_POSTGRES_PASSWORD")
        log_port = os.getenv("LOG_POSTGRES_PORT")

        stg_database = os.getenv("STG_POSTGRES_DB")
        stg_host = os.getenv("STG_POSTGRES_HOST")
        stg_user = os.getenv("STG_POSTGRES_USER")
        stg_password = os.getenv("STG_POSTGRES_PASSWORD")
        stg_port = os.getenv("STG_POSTGRES_PORT")
        
        src_conn = f'postgresql://{src_user}:{src_password}@{src_host}:{src_port}/{src_database}'
        log_conn = f'postgresql://{log_user}:{log_password}@{log_host}:{log_port}/{log_database}'
        stg_conn = f'postgresql://{stg_user}:{stg_password}@{stg_host}:{stg_port}/{stg_database}'
        
        src_engine = create_engine(src_conn)
        log_engine = create_engine(log_conn)
        stg_engine = create_engine(stg_conn)
        
        return src_engine, log_engine, stg_engine
           

    except Exception as e:
        print(f"Error: {e}")
        return None
    
def read_sql(PATH, table_name):
    #open your file .sql
    with open(f"{PATH}{table_name}.sql", 'r') as file:
        content = file.read()
    
    #return query text
    return content