from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

from kaggle import KaggleApi

from datetime import datetime, timedelta
import os
import subprocess

def E_movies():
    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files('rounakbanik/the-movies-dataset', path='./landing_zone', unzip=True, quiet=False)

dag = DAG(

    'movies_load_pkg01',

    default_args={'start_date': days_ago(1)},

    schedule_interval='0 23 * * *',

    catchup=False

)

E_movies_dataset = PythonOperator(
    task_id='E_movies_dataset',
    python_callable=E_movies,
    dag=dag,
)