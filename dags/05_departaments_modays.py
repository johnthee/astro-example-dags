#0 Lectura de librerias
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from time import time_ns
from datetime import datetime , timedelta
from airflow.utils.dates import days_ago
import os
from pymongo import MongoClient
from pandas import DataFrame
from google.cloud import bigquery
import pandas as pd

default_args = {
    'owner': 'Datapath',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# 1 Conections
## 1.1 Conection to mongo
def get_connect_mongo():

    CONNECTION_STRING ="mongodb+srv://atlas:T6.HYX68T8Wr6nT@cluster0.enioytp.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(CONNECTION_STRING)

    return client

def start_process():
    print(" INICIO EL PROCESO!")

def end_process():
    print(" FIN DEL PROCESO!")




def load_departments():
    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["departments"] 
    departments= collection_name.find({})  
    departments_df = DataFrame(departments)
    departments_df['_id'] = departments_df['_id'].astype(str)
    dbconnect.close()

    departments_rows=len(departments_df)
    if departments_rows>0 :
        client = bigquery.Client(project='fluted-clock-411618')
        
    query_string = """
    drop table if exists `fluted-clock-411618.dep_raw.departments` ;
    """
    query_job = client.query(query_string)
    rows = list(query_job.result())
    print(rows)

    departments_rows=len(departments_df)
    if departments_rows>0 :
        client = bigquery.Client(project='fluted-clock-411618')

        table_id =  "fluted-clock-411618.dep_raw.departments"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("department_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("department_name", bigquery.enums.SqlTypeNames.STRING)
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            departments_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla departments')

with DAG(
    dag_id="load_project",
    schedule="0 0 * * MON", 
    start_date=days_ago(2), 
    default_args=default_args
) as dag:
    step_start = PythonOperator(
        task_id='step_start_id',
        python_callable=start_process,
        dag=dag
    )
    fun_load_departments = PythonOperator(
        task_id='load_departments_id',
        python_callable=load_departments,
        dag=dag
    )
    step_end = PythonOperator(
        task_id='step_end_id',
        python_callable=end_process,
        dag=dag
    )

    step_start>>fun_load_departments>>step_end