from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import sys
import os

# ==== Adicionando src/load_dw ao sys.path ====
SRC_PATH = '/opt/airflow/src'
sys.path.append(SRC_PATH)

LOAD_DW_PATH = os.path.join(SRC_PATH, 'load_dw')
sys.path.append(LOAD_DW_PATH)

# ==== Função utilitária para truncar tabelas ====
from sqlalchemy import create_engine

def truncate_table(table):
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')
    with engine.connect() as conn:
        conn.execute(f'TRUNCATE TABLE dw.{table} RESTART IDENTITY CASCADE')
    print(f"[FULL LOAD] Tabela truncada: dw.{table}")

# ==== Importando funções de carga ====
from load_dw.load_dim_location import load_dim_location
from load_dw.load_dim_brewery_type import load_dim_brewery_type
from load_dw.load_dim_brewery_name import load_dim_brewery_name
from load_dw.load_fact_breweries import load_fact_breweries
from load_dw.load_dim_time import load_dim_time

# ==== Configuração da DAG ====
default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='load_dw_breweries_full',
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description='Carga FULL das dimensões e fato no DW'
) as dag:

    # Tasks para truncar cada tabela antes da carga
    t_trunc_dim_location = PythonOperator(
        task_id='truncate_dim_location',
        python_callable=truncate_table,
        op_args=['dim_location']
    )

    t_trunc_dim_time = PythonOperator(
        task_id='truncate_dim_time',
        python_callable=truncate_table,
        op_args=['dim_time']
    )

    t_trunc_dim_type = PythonOperator(
        task_id='truncate_dim_brewery_type',
        python_callable=truncate_table,
        op_args=['dim_brewery_type']
    )

    t_trunc_dim_name = PythonOperator(
        task_id='truncate_dim_brewery_name',
        python_callable=truncate_table,
        op_args=['dim_brewery_name']
    )

    t_trunc_fact = PythonOperator(
        task_id='truncate_fact_breweries',
        python_callable=truncate_table,
        op_args=['fact_breweries']
    )

    # Tasks de carga (após truncamento)
    t_load_dim_location = PythonOperator(
        task_id='load_dim_location',
        python_callable=load_dim_location
    )

    t_load_dim_time = PythonOperator(
        task_id='load_dim_time',
        python_callable=load_dim_time
    )

    t_load_dim_type = PythonOperator(
        task_id='load_dim_brewery_type',
        python_callable=load_dim_brewery_type
    )

    t_load_dim_name = PythonOperator(
        task_id='load_dim_brewery_name',
        python_callable=load_dim_brewery_name
    )

    t_load_fact = PythonOperator(
        task_id='load_fact_breweries',
        python_callable=load_fact_breweries
    )

    # ===== Dependências =====
    trunc_tasks = [t_trunc_dim_location, t_trunc_dim_type, t_trunc_dim_name, t_trunc_dim_time, t_trunc_fact]
    load_tasks = [t_load_dim_location, t_load_dim_type, t_load_dim_name, t_load_dim_time, t_load_fact]

    for trunc_task, load_task in zip(trunc_tasks, load_tasks):
        trunc_task >> load_task
