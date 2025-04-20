from pipeline.staging.extract import extract_database, extract_list_table, extract_sheet
from pipeline.staging.load import load_staging

tables_to_extract = ['acquisition',
                    'company',
                    'funding_rounds',
                    'funds',
                    'investments',
                    'ipos']

excel_to_extract = ['people',
                    'relationships']

extract_data = [
                'acquisition',
                'company',
                'funding_rounds',
                'funds',
                'investments',
                'ipos',
                'people',
                'relationships'
                ]

table_id = [
            'acquisition_id',
            'office_id',
            'funding_round_id',
            'fund_id',
            'investment_id',
            'ipo_id',
            'people_id',
            'relationship_id'
]


##extract database
for index, table_name in enumerate(tables_to_extract):
    table_extract = extract_database(db_name='startup_investments', table_name = table_name)


##extract excel
for index, file_name in enumerate(excel_to_extract):
    excel_extract = extract_sheet(file_name)


##load to staging db
for index, table_name in enumerate(extract_data):
    load_staging(table_name= table_name, idx_name=table_id[index], source='startup investment')    