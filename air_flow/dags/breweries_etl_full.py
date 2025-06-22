from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime
import sys
import os

# ==== Caminhos de scripts Python ====
SRC_PATH = '/opt/airflow/src'
sys.path.append(SRC_PATH)
sys.path.append(os.path.join(SRC_PATH, 'load_dw'))

# ==== Importa funções dos scripts existentes ====
from bronze_layer_full import fetch_raw_data  
from silver_layer_full import transform_and_partition      
from gold_layer_full import generate_gold_files  
from load_dw.load_dim_location import load_dim_location
from load_dw.load_dim_brewery_type import load_dim_brewery_type
from load_dw.load_dim_brewery_name import load_dim_brewery_name
from load_dw.load_fact_breweries import load_fact_breweries

from sqlalchemy import create_engine

# ==== Função utilitária para truncar tabelas ====
def truncate_table(table):
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')
    with engine.connect() as conn:
        conn.execute(f'TRUNCATE TABLE dw.{table} RESTART IDENTITY CASCADE')
    print(f"[FULL LOAD] Tabela truncada: dw.{table}")


def alert_failure(context):
    subject = f"[AIRFLOW] Falha na DAG: {context['task_instance'].dag_id}"
    body = f"""
    Task: {context['task_instance'].task_id}
    DAG: {context['task_instance'].dag_id}
    Execução: {context['execution_date']}
    Log: {context['task_instance'].log_url}
    """
    send_email(to="seu.email@exemplo.com", subject=subject, html_content=body)

def alert_success(context):
    subject = f"[AIRFLOW] Sucesso na DAG: {context['task_instance'].dag_id}"
    body = f"""
    Task: {context['task_instance'].task_id}
    DAG: {context['task_instance'].dag_id}
    Execução: {context['execution_date']}
    Log: {context['task_instance'].log_url}
    """
    send_email(to="seu.email@exemplo.com", subject=subject, html_content=body)


default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'email': ['carlos.soaresti@gmail.com'],       # <-- SEU E-MAIL aqui
    'email_on_failure': True,                     # Envia e-mail em falha
    'email_on_retry': False,                      # Não envia em retry
    'email_on_success': False                     # (Opcional) Sucesso - normalmente False
}

with DAG(
    dag_id='breweries_etl_full',
    schedule_interval='0 1 * * *',  # Diariamente às 01:00
    catchup=False,
    default_args=default_args,
    description='Pipeline ETL completo breweries',
    on_failure_callback=alert_failure,
    on_success_callback=alert_success
) as dag:

    # ========== ETL RAW (Bronze -> Silver -> Gold) ==========
    t_fetch_raw_data = PythonOperator(
        task_id='fetch_raw_data',
        python_callable=fetch_raw_data
    )

    t_transform_and_partition = PythonOperator(
        task_id='transform_and_partition',
        python_callable=transform_and_partition
    )

    t_generate_gold_files = PythonOperator(
        task_id='generate_gold_files',
        python_callable=generate_gold_files
    )

    # ========== TRUNCATE TABELAS DW ==========
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

    # ========== CARGA DIMENSÕES ==========
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

    # ========== CARGA FATO ==========
    t_load_fact = PythonOperator(
        task_id='load_fact_breweries',
        python_callable=load_fact_breweries
    )

    # ========== Dependências (Orquestração) ==========
    # Pipeline Bronze → Silver → Gold
    t_fetch_raw_data >> t_transform_and_partition >> t_generate_gold_files

    # Depois da ingestão, truncar DW
    [t_generate_gold_files >> t_trunc_dim_location,
     t_generate_gold_files >> t_trunc_dim_type,
     t_generate_gold_files >> t_trunc_dim_name]

    # Cada truncate antes da respectiva carga
    t_trunc_dim_location >> t_load_dim_location
    t_trunc_dim_type >> t_load_dim_type
    t_trunc_dim_name >> t_load_dim_name

    # Depois de TODAS dimensões carregadas, truncar a fato
    [t_load_dim_location, t_load_dim_type, t_load_dim_name] >> t_trunc_fact

    # Carregar a fato por último
    t_trunc_fact >> t_load_fact
