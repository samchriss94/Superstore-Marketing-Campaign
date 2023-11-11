'''
=================================================
Milestone 3

Nama  : Samuel Christian S
Batch : SBY-001

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Adapun dataset yang dipakai adalah dataset mengenai penjualan mobil di Indonesia selama tahun 2020.
=================================================
'''


import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

import datetime as dt 
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os.path

def get_data():
    # conn_string="dbname='ftds' host='localhost' user='postgres' password='scs1638' port=5432"
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)

    SQL = '''SELECT "Id", "Year_Birth", "Education", "Marital_Status", "Income", "Kidhome", "Teenhome", "Dt_Customer", "Recency", 
        "MntWines", "MntFruits", "MntMeatProducts", "MntFishProducts", "MntSweetProducts", "MntGoldProds", 
        "NumDealsPurchases", "NumWebPurchases", "NumCatalogPurchases", "NumStorePurchases", "NumWebVisitsMonth", 
        "Response", "Complain"
        FROM table_m3'''

    df = pd.read_sql(SQL, conn)
    df.to_csv('/opt/airflow/data/P2M3_samuel_christian_data_raw.csv', index=False)
 

def normalcolumn():
    df = pd.read_csv('/opt/airflow/data/P2M3_samuel_christian_data_raw.csv', index_col=False)
    # df = df.copy()
    df = df.dropna()
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ','_')
    df['income'] = df['income'].astype(float)
    df.to_csv('/opt/airflow/data/P2M3_samuel_christian_data_clean.csv', index=False)
    


def import_data():

    df = pd.read_csv('/opt/airflow/data/P2M3_samuel_christian_data_clean.csv', index_col=False)
    es = Elasticsearch("http://localhost:9200") 
    es.ping()

    
# default_args = {
#     'owner': 'samuel',
#     'start_date': dt.datetime(2023, 11, 3, 16, 10, 0) - dt.timedelta(hours=7),
#     # 'retries': 1,
#     # 'retry_delay': dt.timedelta(minutes=5),
# }

default_args = {
    'owner': 'samuel',
    'start_date': dt.datetime(2023, 11, 3, 13, 30, 0) - dt.timedelta(hours=7),
}

with DAG('Mile1_3',
         default_args=default_args,
         schedule_interval= '30 6 * * *',
         )as dag:


    getdatapos = PythonOperator(task_id='getdatapos', python_callable=get_data)

    cldata = PythonOperator(task_id='cldata', python_callable=normalcolumn)

    elasticimp = PythonOperator(task_id='elasticimp', python_callable=import_data)

getdatapos >> cldata >> elasticimp