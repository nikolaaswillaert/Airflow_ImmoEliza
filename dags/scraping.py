from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import utils.get_dataset as getdata
import utils.clean_dataset as cleandata
import utils.model_train as train_model
import requests
import pandas as pd
from airflow.hooks.S3_hook import S3Hook

#### #### ##
def upload_to_s3(filename:str, key: str, bucket_name: str) ->None:
    # Upload the created model onto S3 bucket
    # hook = S3Hook('S3_conn')
    hook = S3Hook('minios3')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

default_args={
    'owner':'niko',
    'retries':5,
    'retry_delay':timedelta(minutes=3)
}

with DAG(
    default_args=default_args,
    dag_id='Scrapersv1',
    start_date=datetime(2023, 9, 19),
    schedule_interval='0 20 * * *',
    catchup=True
) as dag:
    
    create_df = PythonOperator(
        task_id='create_dataframe',
        python_callable=getdata.create_dataframe,
    )

    clean_df = PythonOperator(
        task_id='clean_dataframe',
        python_callable=cleandata.clean_dataset,
    )

    upload_df_to_s3 = PythonOperator(
        task_id='upload_dataframe_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename':'data_output/history/cleaned_dataframe_{{ ds }}.csv',
            'key':'cleaned_df_{{ ds_nodash }}.csv',
            'bucket_name':'cleandatas3'
        }
    )
    
    train_xgb_model = PythonOperator(
        task_id='train_xgb_model',
        python_callable=train_model.train_model
    )

    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename':'data_output/history/xgb_model_{{ ds }}.joblib',
            'key':'xgb_model_{{ ds_nodash }}.joblib',
            'bucket_name':'xgbmodels3'
        }
    )

create_df >> clean_df >> upload_df_to_s3 >> train_xgb_model >> task_upload_to_s3