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
## 1.2 Conection to big query
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/JCAMPOS/Desktop/DE DATA PATH/Módulo 8. Data Engineering  with Python/fluted-clock-411618-1e535991c221.json"

#2 Dags functions
    
def start_process():
    print(" INICIO EL PROCESO!")

def end_process():
    print(" FIN DEL PROCESO!")

# 3 Tranformation functions

##3.1 Date transformation
def transform_date(text):
    print(f"Ejecutando transform_date")
    text = str(text)
    d = text[0:10]
    return d

## 3.1 Group status transformation
def get_group_status(text):
    text = str(text)
    if text =='CLOSED':
        d='END'
    elif text =='COMPLETE':
        d='END'
    else :
        d='TRANSIT'
    return d

def multiplicacion(x):
    return x*tipcambio_df.compra[0]

#4 Load functions
def load_orders():
    #Orders viene nativo de mongo
    print(f" INICIO LOAD ORDERS")
    client = bigquery.Client(project='fluted-clock-411618')
    
    query_string = """
    drop table if exists `fluted-clock-411618.dep_raw.orders` ;
    """
    query_job = client.query(query_string)
    print('dropped existing orders')
    rows = list(query_job.result())
    print(rows)
    dbconnect = get_connect_mongo()
    print('succesfully conected to mongo')
    dbname=dbconnect["retail_db"]
    collection_name = dbname["orders"] 
    orders = collection_name.find({})  
    orders_df = DataFrame(orders)
    dbconnect.close()
    orders_df['_id'] = orders_df['_id'].astype(str)
    orders_df['order_date']  = orders_df['order_date'].map(transform_date)
    orders_df['order_date'] = pd.to_datetime(orders_df['order_date'], format='%Y-%m-%d').dt.date
    orders_rows=len(orders_df)
    if orders_rows>0 :
        client = bigquery.Client(project='fluted-clock-411618')

        table_id =  "fluted-clock-411618.dep_raw.orders"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("order_customer_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_status", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            orders_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla orders')


def load_order_items():
    #order_items viene nativo de mongo
    print(f" INICIO LOAD ORDER_ITEMS")
    client = bigquery.Client(project='fluted-clock-411618')
    query_string = """
    drop table if exists `fluted-clock-411618.dep_raw.order_items` ;
    """
    query_job = client.query(query_string)
    rows = list(query_job.result())
    print(rows)
    dbconnect = get_connect_mongo()
    print('Succesfully connected to mongo for order_items')
    dbname=dbconnect["retail_db"]
    collection_name = dbname["order_items"] 
    order_items = collection_name.find({})  
    order_items_df = DataFrame(order_items)
    dbconnect.close()
    order_items_df['_id'] = order_items_df['_id'].astype(str)
    order_items_df['order_date']  = order_items_df['order_date'].map(transform_date)
    order_items_df['order_date'] = pd.to_datetime(order_items_df['order_date'], format='%Y-%m-%d').dt.date

    order_items_rows=len(order_items_df)
    if order_items_rows>0 :
        client = bigquery.Client(project='fluted-clock-411618')

        table_id =  "fluted-clock-411618.dep_raw.order_items"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("order_date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("order_item_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_product_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_quantity", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_subtotal", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("order_item_product_price", bigquery.enums.SqlTypeNames.FLOAT),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            order_items_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla order_items')
    

def load_products():
    print(f" INICIO LOAD PRODUCTS")
    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["products"] 
    products = collection_name.find({})
    products_df = DataFrame(products)
    dbconnect.close()
    products_df['_id'] = products_df['_id'].astype(str)
    products_df['product_description'] = products_df['product_description'].astype(str)
    products_rows=len(products_df)
    print(f" Se obtuvo  {products_rows}  Filas")
    products_rows=len(products_df)
    if products_rows>0 :
        client = bigquery.Client(project='fluted-clock-411618')
        table_id =  "fluted-clock-411618.dep_raw.products"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("product_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("product_category_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("product_name", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("product_description", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("product_price", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("product_image", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            products_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla productos')

def load_customers():
    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["customers"] 

    # customers = collection_name.find({},{"_id":1,"customer_id":1})  filtrar columna
    customers = collection_name.find({})
    customers_df = DataFrame(customers)
    dbconnect.close()
    customers_df['_id'] = customers_df['_id'].astype(str)
    customers_rows=len(customers_df)
    if customers_rows>0 :
        client = bigquery.Client(project='fluted-clock-411618')

        table_id =  "fluted-clock-411618.dep_raw.customers"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("customer_fname", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_lname", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_email", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_password", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_street", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_city", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_state", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_zipcode", bigquery.enums.SqlTypeNames.INTEGER),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            customers_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla customers')

def load_categories():
    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["categories"] 
    categories= collection_name.find({})  
    categories_df = DataFrame(categories)
    dbconnect.close()
    categories_rows=len(categories_df)
    if categories_rows>0 :
        client = bigquery.Client(project='fluted-clock-411618')

        table_id =  "fluted-clock-411618.dep_raw.categories"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("category_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("category_department_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("category_name", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            categories_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla categories')

def load_departments():
    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["departments"] 
    departments= collection_name.find({})  
    departments_df = DataFrame(departments)
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

def build_master():
    client = bigquery.Client(project='fluted-clock-411618')
    query_string = """
    drop table if exists `fluted-clock-411618.dep_raw.master_order` ;
    """
    query_job = client.query(query_string)
    rows = list(query_job.result())
    print(rows)

    sql = """
        SELECT *
        FROM `fluted-clock-411618.dep_raw.order_items`
    """
    m_order_items_df = client.query(sql).to_dataframe()
    sql_2 = """
        SELECT *
        FROM `fluted-clock-411618.dep_raw.orders`
    """
    m_orders_df = client.query(sql_2).to_dataframe()
    df_join = m_orders_df.merge(m_order_items_df, left_on='order_id', right_on='order_item_order_id', how='inner')
    df_master=df_join[[ 'order_id', 'order_date_x', 'order_customer_id',
       'order_status',  'order_item_id',
       'order_item_order_id', 'order_item_product_id', 'order_item_quantity',
       'order_item_subtotal', 'order_item_product_price']]
    df_master=df_master.rename(columns={"order_date_x":"order_date"})
    df_master['order_date'] = df_master['order_date'].astype(str)
    df_master['order_date'] = pd.to_datetime(df_master['order_date'], format='%Y-%m-%d').dt.date

    headers_files = {
    'tipocambio':["fecha","compra","venta","error"]
        }
    dwn_url_tipcambio='https://www.sunat.gob.pe/a/txt/tipoCambio.txt'
    tipcambio_df = pd.read_csv(dwn_url_tipcambio, names=headers_files['tipocambio'], sep='|')
    tipcambio_df = tipcambio_df.drop(columns=['error'])
    df_master['order_item_subtotal_mn']  = df_master['order_item_subtotal'].map(multiplicacion)

    df_master_rows=len(df_master)
    if df_master_rows>0 :
        client = bigquery.Client(project='fluted-clock-411618')

        table_id =  "fluted-clock-411618.dep_raw.master_order"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("order_customer_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_status", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("order_item_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_product_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_quantity", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_subtotal", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("order_item_product_price", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("order_item_subtotal_mn", bigquery.enums.SqlTypeNames.FLOAT),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            df_master, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla order_items')


with DAG(
    dag_id="load_project",
    schedule="20 04 * * *", 
    start_date=days_ago(2), 
    default_args=default_args
) as dag:
    step_start = PythonOperator(
        task_id='step_start_id',
        python_callable=start_process,
        dag=dag
    )
    fun_load_products = PythonOperator(
        task_id='load_products_id',
        python_callable= load_products,
        dag=dag
    )
    fun_load_customers = PythonOperator(
        task_id='load_customers_id',
        python_callable=load_customers,
        dag=dag
    )
    fun_load_orders= PythonOperator(
        task_id='load_orders_id',
        python_callable=load_orders,
        dag=dag
    )
    fun_load_order_items= PythonOperator(
        task_id='load_order_items_id',
        python_callable=load_order_items,
        dag=dag
    )
    fun_load_categories = PythonOperator(
        task_id='load_categories_id',
        python_callable=load_categories,
        dag=dag
    )
    fun_load_departments = PythonOperator(
        task_id='load_departments_id',
        python_callable=load_departments,
        dag=dag
    )
    fun_build_master= PythonOperator(
        task_id='build_master_id',
        python_callable=build_master,
        dag=dag
    )
    step_end = PythonOperator(
        task_id='step_end_id',
        python_callable=end_process,
        dag=dag
    )
    step_start>>fun_load_products>>fun_load_customers>>fun_load_orders>>fun_load_order_items>>fun_load_categories>>fun_load_departments>>fun_build_master>>step_end