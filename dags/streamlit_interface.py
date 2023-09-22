from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import utils.get_dataset as getdata
import utils.clean_dataset as cleandata
import utils.model_train as train_model
import requests
import pandas as pd
from airflow.hooks.S3_hook import S3Hook

default_args={
    'owner':'niko',
    'retries':5,
    'retry_delay':timedelta(minutes=10)
}

with DAG(
    default_args=default_args,
    dag_id='StreamlitWebServerv1',
    start_date=datetime(2023, 9, 19),
    schedule_interval='0 20 * * *',
    catchup=False
) as dag:
    create_streamlit = BashOperator(
        task_id='startStreamlitServer',
        bash_command='streamlit run /opt/airflow/dags/utils/streamlitapp.py',
    )
