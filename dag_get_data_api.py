from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
#  from airflow.hooks.mysql import MySqlHook
from datetime import datetime
# from airflow.hooks.mysql_hook import MySqlHook

import pandas as pd
from sqlalchemy import create_engine

default_args = {
    'owner': 'checkatrade',
    'depend_on_past': False,
    'start_date': datetime(year=2022, month=3, day=8),
    'retries': 0,
}

def create_mysql_table():
    query = """CREATE TABLE results_peoplw
        (
            ID_People int  NOT NULL AUTO_INCREMENT,
            name varchar(20),
            height int,
            mass int,
            hair_color varchar(15),
            skin_color varchar(15),
            eye_color varchar(15),
            birth_year varchar(10), 
            Gender varchar(7), 
            Homeworld varchar(50),
            Species varchar(50),
            Created varchar(25),
            Edited varchar(25),
            url varchar(25)
        );"""
    hook = MySqlHook(mysql_conn_id='mysql_new',
                        host='localhost',
                        database='airflow_db',
                        user='root',
                        password='rootroot',
                        port=3306)

    connection = hook.get_conn()
    # COMMENTED OUT FOR DEBUGGING
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()

# Similarly we can create all other 3 tables(results_films, results_vehicles, results_startships that I written in the document

def get_data_from_api():
    da=pd.read_json("https://swapi.dev/api/people")
    engine = create_engine('mysql://root:rootroot@localhost:mysql')
    da.to_sql('results_people', engine)


dag = DAG(
    "get_data_from_api",
    default_args=default_args,
    schedule_interval='* * * * *'
)

start_task = DummyOperator(task_id='start_task', dag=dag)
postgres_task = PythonOperator(task_id='create_table',
                               python_callable=create_mysql_table,
                               dag=dag)

get_data = PythonOperator(task_id='get_data_from_api',
                          python_callable=get_data_from_api,
                          dag=dag)
start_task >> get_data
