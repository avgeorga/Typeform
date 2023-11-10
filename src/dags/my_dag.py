from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from datetime import datetime
import etl_form, etl_formevents

def _etl_form ():
    etl_form.main()

def _etl_form_events():
    etl_formevents.main()

with DAG("etl_typeform", 
         start_date=datetime(2023,11, 10), 
         schedule_interval="@once",
         catchup=False ) as dag:
    
    typerform_etl_form = PythonOperator(
        task_id="etl_form",
        python_callable=_etl_form
    )   

    typerform_etl_form_events = PythonOperator(
        task_id="etl_form_events",
        python_callable=_etl_form_events
    )   

[typerform_etl_form, typerform_etl_form]