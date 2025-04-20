from src.staging.extract.extract_database import extract_database
from src.staging.load.load import load_staging
from src.staging.extract.extract_spreadsheet import extract_spreadsheet


def pipeline_staging():
    print("==== Start Staging Pipeline ===")

    # Extract data from database clinic
    df_speciality = extract_database(db_name='clinic', table_name = "speciality")
    df_doctor = extract_database(db_name='clinic', table_name = "doctor")
    df_patient = extract_database(db_name='clinic', table_name = "patient")
    df_medication = extract_database(db_name='clinic', table_name = "medication")
    df_appointment = extract_database(db_name='clinic', table_name = "appointment")
    df_prescription = extract_database(db_name='clinic', table_name = "prescription")

    # Load data to staging
    load_staging(data=df_speciality, table_name='speciality', schema='public', idx_name='speciality_id', source='database clinic')
    load_staging(data=df_doctor, table_name='doctor', schema='public', idx_name='doctor_id', source='database clinic')
    load_staging(data=df_patient, table_name='patient', schema='public', idx_name='patient_id', source='database clinic')
    load_staging(data=df_medication, table_name='medication', schema='public', idx_name='medication_id', source='database clinic')
    load_staging(data=df_appointment, table_name='appointment', schema='public', idx_name='appointment_id', source='database clinic')
    load_staging(data=df_prescription, table_name='prescription', schema='public', idx_name='prescription_id', source='database clinic')

    # Extract data from database clinic_ops
    df_speciality = extract_database(db_name='clinic_ops', table_name='speciality_ops')
    df_role = extract_database(db_name='clinic_ops', table_name='role')
    df_employee = extract_database(db_name='clinic_ops', table_name='employee')
    df_salary = extract_database(db_name='clinic_ops', table_name='salary')
    df_leave_requests = extract_database(db_name='clinic_ops', table_name='leave_requests')
    df_equipment = extract_database(db_name='clinic_ops', table_name='equipment')
    df_maintenance_record = extract_database(db_name='clinic_ops', table_name='maintenance_record')

    # Load data to staging
    load_staging(data=df_speciality, table_name='speciality_ops', schema='public', idx_name='speciality_id', source='database clinic_ops')
    load_staging(data=df_role, table_name='role', schema='public', idx_name='role_id', source='database clinic_ops')
    load_staging(data=df_employee, table_name='employee', schema='public', idx_name='employee_id', source='database clinic_ops')
    load_staging(data=df_salary, table_name='salary', schema='public', idx_name='salary_id', source='database clinic_ops')
    load_staging(data=df_leave_requests, table_name='leave_requests', schema='public', idx_name='leave_id', source='database clinic_ops')
    load_staging(data=df_equipment, table_name='equipment', schema='public', idx_name='equipment_id', source='database clinic_ops')
    load_staging(data=df_maintenance_record, table_name='maintenance_record', schema='public', idx_name='record_id', source='database clinic_ops')

    # Extract data from spreadsheet
    df_maintenance_request = extract_spreadsheet('maintenance_request')

    # index as new column and identifier
    df_maintenance_request = df_maintenance_request.reset_index()
    df_maintenance_request.rename(columns={'index':'id'}, inplace=True)

    # Load data to staging
    load_staging(data=df_maintenance_request, table_name='maintenance_request', schema='public', idx_name='id', source='spreadsheet')

    print("==== End Staging Pipeline ===")