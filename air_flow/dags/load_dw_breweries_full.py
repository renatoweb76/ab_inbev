from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from datetime import datetime
import sys
import os

# ==== Adicionando src/load_dw ao sys.path ====
SRC_PATH = '/opt/airflow/src'
sys.path.append(SRC_PATH)
LOAD_DW_PATH = os.path.join(SRC_PATH, 'load_dw')
sys.path.append(LOAD_DW_PATH)

from sqlalchemy import create_engine

def truncate_table(table):
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')
    with engine.connect() as conn:
        conn.execute(f'TRUNCATE TABLE dw.{table} RESTART IDENTITY CASCADE')
    print(f"[FULL LOAD] Tabela truncada: dw.{table}")

from load_dw.load_dim_location import load_dim_location
from load_dw.load_dim_brewery_type import load_dim_brewery_type
from load_dw.load_dim_brewery_name import load_dim_brewery_name
from load_dw.load_fact_breweries import load_fact_breweries

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

    # 1. Aguarda a DAG de ingestão terminar
    wait_ingestion = ExternalTaskSensor(
        task_id='wait_for_ingestion_breweries',
        external_dag_id='ingestion_breweries',   # O DAG ID que deve ser aguardado
        external_task_id=None,                    # None = DAG inteira precisa estar finalizada com sucesso
        mode='poke',
        poke_interval=60,                        # verifica a cada 60 segundos
        timeout=60*60,                           # timeout de 1h (ajuste se necessário)
        dag=dag
    )

    # 2. Truncamento das tabelas
    t_trunc_dim_location = PythonOperator(
        task_id='truncate_dim_location',
        python_callable=truncate_table,
        op_args=['dim_location']
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

    # 3. Carga das dimensões e fato
    t_load_dim_location = PythonOperator(
        task_id='load_dim_location',
        python_callable=load_dim_location
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

    # ===== Dependências (agora tudo começa após o sensor) =====
    wait_ingestion >> t_trunc_dim_location
    wait_ingestion >> t_trunc_dim_type
    wait_ingestion >> t_trunc_dim_name

    t_trunc_dim_location >> t_load_dim_location
    t_trunc_dim_type >> t_load_dim_type
    t_trunc_dim_name >> t_load_dim_name

    [t_load_dim_location, t_load_dim_type, t_load_dim_name] >> t_trunc_fact
    t_trunc_fact >> t_load_fact
