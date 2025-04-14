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

        stg_database = os.getenv("STG_POSTGRES_DB")
        stg_host = os.getenv("STG_POSTGRES_HOST")
        stg_user = os.getenv("STG_POSTGRES_USER")
        stg_password = os.getenv("STG_POSTGRES_PASSWORD")
        stg_port = os.getenv("STG_POSTGRES_PORT")
        
        src_conn = f'postgresql://{src_user}:{src_password}@{src_host}:{src_port}/{src_database}'
        stg_conn = f'postgresql://{stg_user}:{stg_password}@{stg_host}:{stg_port}/{stg_database}'
        
        src_engine = create_engine(src_conn)
        stg_engine = create_engine(stg_conn)
        
        return src_engine, stg_engine

    except Exception as e:
        print(f"Error: {e}")
        return None