import airflow
from airflow import DAG
from datetime import timedelta,datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
import os

tmpl_path = []

for subdir in ['template_path'] :
    tmpl_path.append(os.path.join('/home/ajay/airflow',subdir))

default_args = {
    'owner' : 'ajay',
    'start_date' : datetime(2023,7,11),
    'email' : ['kc.ajay1993@gmail.com'],
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 2,
    'retry_delay' : timedelta(minutes=5),
}

version = 1

dag = DAG(
    dag_id=f'Auto-index-tuning-MongoDB-v{version}',
    default_args=default_args,
    description='Auto Index tuning process for MongoDB',
    schedule_interval=timedelta(days=1),
    template_searchpath=tmpl_path,
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

index_selection = BashOperator(
    task_id='index_selection',
    bash_command='python3 /home/ajay/airflow/template_path/main.py',
    dag=dag,
)

start>>index_selection
