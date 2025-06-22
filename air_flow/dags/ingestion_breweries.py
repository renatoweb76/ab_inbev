from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import sys
import os

# ==== Adicionando src ao sys.path ====

SRC_PATH = '/opt/airflow/src'
sys.path.append(SRC_PATH)

# ==== Importando funções principais ====

from bronze_layer_full import fetch_raw_data
from silver_layer_full import transform_and_partition
from gold_layer_full import generate_gold_files

# ==== Configuração da DAG ====

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='ingestion_breweries',
    schedule_interval=None,  
    catchup=False,
    default_args=default_args,
    description='Pipeline ETL de ingestão das cervejarias (bronze, silver, gold)'
) as dag:

    # Bronze
    t_bronze = PythonOperator(
        task_id='fetch_raw_data',
        python_callable=fetch_raw_data
    )

    # Silver
    t_silver = PythonOperator(
        task_id='transform_to_silver',
        python_callable=transform_and_partition
    )

    # Gold
    t_gold = PythonOperator(
        task_id='generate_gold',
        python_callable=generate_gold_files
    )

    # Dependências entre tarefas
    t_bronze >> t_silver >> t_gold
