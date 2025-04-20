from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import csv
from datetime import datetime
from utils.db_conn import db_connection


def extract_list_table(db_name):
    try:
        # Define db connection engine
        src_engine,_ = db_connection()
        
        # Get list of tables in the database
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        df = pd.read_sql(sql=query, con=src_engine)
        return df
    except Exception as e:
        print(e)
        return None

def extract_database(db_name, table_name: str): 
    try:
        # Define db connection engine
        src_engine = db_connection()

        # Constructs a SQL query to select all columns from the specified table_name
        query = f"SELECT * FROM {table_name}"

        # Execute the query with pd.read_sql
        df = pd.read_sql(sql=query, con=src_engine)
        return df
    except Exception as e:
        print(e)
        return None

    