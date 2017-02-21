from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators import PythonOperator
from datetime import datetime, timedelta



default_args = {
     'start_date': ,
    'email': ['jsimcock@rhg.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval'='0 0 * * *',
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    #'end_date': datetime(2016, 1, 1),
}


dag = DAG('dynamo_backup_to_osdc', default_args=default_args)

t2 = BashOperator(
    task_id='backup_dynamo',
    bash_command='python ~/airflow/dags/dynamo_backup.py',
    dag=dag)