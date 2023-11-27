from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

dag =  DAG(
    'phtu_dag01',
    default_args={
        'email': ['phtu.hazard@gmail.com'],
        'email_on_failure': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='A simple DAG sample by phtu',
    schedule_interval="@once",
    start_date=datetime(2023, 11, 26), # Start date
    tags=['phtu'],
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date >> /Users/tuphanhoang/Workspace/Big_Data/date.txt',
    dag = dag
)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag = dag
)
t3 = BashOperator(
    task_id='echo',
    bash_command='echo t3 running',
    dag = dag
)

[t1 , t2] >> t3
