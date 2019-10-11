from airflow import DAG
from airflow.operators import BashOperator
from datetime import timedelta
import keyconfigs as cfg

default_args = cfg.airflow

dag = DAG('esports', default_args=default_args, schedule_interval=timedelta(days=1))

t1 = BashOperator(
    task_id='writeDB',
    bash_command='python ~/data_ingest/getMatches.py',
    dag=dag)

t2 = BashOperator(
    task_id='etl',
    bash_command='python ~/data_processing/matchlistETL.py',
    dag=dag)

t3 = BashOperator(
    task_id='writeDB',
    bash_command='python ~/database_scripts/writeDB.py',
    dag=dag)


t1 >> t2 >> t3
