# Create Reporting Profiling
import pandas as pd
import json
import re

def extract_sheet(file_name: str): 
    try:
        # read from file
        df_result = pd.read_csv(file_name)

        return df_result
    except Exception as e:
        print(e)
        return None