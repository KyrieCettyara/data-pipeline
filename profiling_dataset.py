from profiling.profile import Profiling
from profiling.extract.extract_db import extract_database
from profiling.extract.extract_db import extract_list_table
from profiling.extract.extract_sheet import extract_sheet

import os
from dotenv import load_dotenv

load_dotenv()

# Define DIR
SRC_POSTGRES_DB = os.getenv("SRC_POSTGRES_DB")

##profiling database
# Extract list of table in database
list_table = extract_list_table(db_name=SRC_POSTGRES_DB)

# Profiling Table company
df_company = extract_database(SRC_POSTGRES_DB, 'company')

# create profiling object
complany_profiling = Profiling(data = df_company, table_name='company')

# get columns from the table
complany_profiling.get_columns()

# Set Profiling Rule
# list check data type (all columns)
data_type_column = complany_profiling.get_columns()

#list check unique values
unique_values = []

#list check percentage missing values
missing_values = ['country_code', 'state_code', 'region']

#list check valid date values
valid_date = ['created_at','updated_at']

# Set Profiling rule to object
complany_profiling.selected_columns(data_type_column, unique_values, missing_values, valid_date)

# Create Reporting Profiling
report_company = complany_profiling.reporting()


# Profiling Table acquisition
df_acquisition = extract_database(SRC_POSTGRES_DB, 'acquisition')

# create profiling object
acquisition_profiling = Profiling(data = df_acquisition, table_name='acquisition')

# get columns from the table
acquisition_profiling.get_columns()

# Set Profiling Rule
# list check data type (all columns)
data_type_column = acquisition_profiling.get_columns()

#list check unique values
unique_values = []

#list check percentage missing values
missing_values = ['term_code', 'price_amount', 'price_currency_code', 'acquired_at', 'source_url']

#list check valid date values
valid_date = ['acquired_at','created_at','updated_at']

# Set Profiling rule to object
acquisition_profiling.selected_columns(data_type_column, unique_values, missing_values, valid_date)

# Create Reporting Profiling
report_company = acquisition_profiling.reporting()


##profiling sheet
# Extract data from spreadsheet
df_people = extract_sheet('../data-pipeline/script/data/people.csv')

people_profiling = Profiling(data = df_people, table_name='people')

# get columns from the table
people_profiling.get_columns()

# Set Profiling Rule
# list check data type (all columns)
data_type_column = people_profiling.get_columns()

#list check unique values
unique_values_column = []

#list check percentage missing values
missing_values_column = ['first_name','last_name','birthplace']

#list check valid date values
valid_date_column = []

# Set Profiling rule to object
people_profiling.selected_columns(data_type_column, unique_values_column, missing_values_column, valid_date_column)

# Create Reporting Profiling
report_maintenance_request = people_profiling.reporting()

# Extract data from spreadsheet
df_relationships = extract_sheet('../data-pipeline/script/data/relationships.csv')

relationships_profiling = Profiling(data = df_relationships, table_name='relationships')

# get columns from the table
relationships_profiling.get_columns()

# Set Profiling Rule
# list check data type (all columns)
data_type_column = relationships_profiling.get_columns()

#list check unique values
unique_values_column = []

#list check percentage missing values
missing_values_column = ['person_object_id','title','relationship_object_id']

#list check valid date values
valid_date_column = ['start_at','end_at']

# Set Profiling rule to object
relationships_profiling.selected_columns(data_type_column, unique_values_column, missing_values_column, valid_date_column)

# Create Reporting Profiling
report_maintenance_request = relationships_profiling.reporting()


