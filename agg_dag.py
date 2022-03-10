
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.mysql.operator import MySqlOperator
import pandas as pd
from sqlalchemy import create_engine

default_args = {
    'owner': 'checkatrade',
    'depend_on_past': False,
    'start_date': datetime(year=2022, month=3, day=8),
    'retries': 0
}

child_task1 = ExternalTaskSensor(
    task_id="child_task1",
    external_dag_id='get_data_from_api',
    external_task_id='get_data_from_api',
    timeout=600,
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    mode="reschedule",
)

mysql_task = MySqlOperator(dag=dag,
                           mysql_conn_id='mysql_default', 
                           task_id='mysql_task',
                           sql='checkatrade_sql.sql',
                           params={'test_user_id': -99})


child_task1 >> mysql_task

