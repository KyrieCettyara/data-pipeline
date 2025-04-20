from pipeline.dwh.extract import extract_database
from pipeline.dwh.transform import transform_commpany,transform_funding_rounds
from pipeline.dwh.load import load_warehouse


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

##extract data from staging database
for index, table_name in enumerate(extract_data):
    table_extract = extract_database(table_name = table_name)

##transform data
company_transform = transform_commpany(table_name='company')
funding_rounds_transform = transform_funding_rounds(table_name='funding_rounds')

##load data
load_warehouse(data=company_transform, table_name='dim_company', idx_name='office_nk', table_process='company', source='staging')
load_warehouse(data=funding_rounds_transform, table_name='dim_funding_rounds', idx_name='funding_round_nk', table_process='funding_rounds', source='staging')


