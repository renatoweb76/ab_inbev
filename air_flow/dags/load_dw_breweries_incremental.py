from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# ==== Adicionando src/load_dw ao sys.path ====
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_PATH = os.path.join(BASE_DIR, 'src')
LOAD_DW_PATH = os.path.join(SRC_PATH, 'load_dw')
sys.path.append(LOAD_DW_PATH)

from sqlalchemy import create_engine
import pandas as pd

# ======================
# Funções de carga incremental
# ======================

def incremental_load_dim_location():
    parquet_path = '/files/gold/dim_location.parquet'
    df_new = pd.read_parquet(parquet_path)
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')
    df_existing = pd.read_sql('SELECT city, state, country FROM dw.dim_location', engine)

    # Mantém só os novos registros
    df_to_insert = df_new.merge(df_existing, on=['city', 'state', 'country'], how='left', indicator=True)
    df_to_insert = df_to_insert[df_to_insert['_merge'] == 'left_only'].drop('_merge', axis=1)

    if not df_to_insert.empty:
        df_to_insert.to_sql('dim_location', engine, schema='dw', if_exists='append', index=False)
        print(f"[INCREMENTAL] {len(df_to_insert)} novas localizações inseridas.")
    else:
        print("[INCREMENTAL] Nenhuma nova localização a inserir.")

# Carga incremental da dimensão de tipo de cervejaria
def incremental_load_dim_brewery_type():
    parquet_path = '/files/gold/dim_brewery_type.parquet'
    df_new = pd.read_parquet(parquet_path)
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')
    df_existing = pd.read_sql('SELECT brewery_type FROM dw.dim_brewery_type', engine)

    df_to_insert = df_new.merge(df_existing, on=['brewery_type'], how='left', indicator=True)
    df_to_insert = df_to_insert[df_to_insert['_merge'] == 'left_only'].drop('_merge', axis=1)

    if not df_to_insert.empty:
        df_to_insert.to_sql('dim_brewery_type', engine, schema='dw', if_exists='append', index=False)
        print(f"[INCREMENTAL] {len(df_to_insert)} novos tipos de cervejaria inseridos.")
    else:
        print("[INCREMENTAL] Nenhum novo tipo de cervejaria a inserir.")

# Carga incremental da dimensão de nome de cervejaria
def incremental_load_dim_brewery_name():
    parquet_path = '/files/gold/dim_brewery_name.parquet'
    df_new = pd.read_parquet(parquet_path)
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')
    df_existing = pd.read_sql('SELECT api_brewery_id FROM dw.dim_brewery_name', engine)

    df_to_insert = df_new.merge(df_existing, on=['api_brewery_id'], how='left', indicator=True)
    df_to_insert = df_to_insert[df_to_insert['_merge'] == 'left_only'].drop('_merge', axis=1)

    if not df_to_insert.empty:
        df_to_insert.to_sql('dim_brewery_name', engine, schema='dw', if_exists='append', index=False)
        print(f"[INCREMENTAL] {len(df_to_insert)} novos nomes de cervejaria inseridos.")
    else:
        print("[INCREMENTAL] Nenhum novo nome de cervejaria a inserir.")

# carga incremental da dimensão de tempo
def incremental_load_dim_time():
    parquet_path = '/files/gold/dim_time.parquet'
    df_new = pd.read_parquet(parquet_path)
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')
    df_existing = pd.read_sql('SELECT full_date FROM dw.dim_time', engine)

    df_to_insert = df_new.merge(df_existing, on=['full_date'], how='left', indicator=True)
    df_to_insert = df_to_insert[df_to_insert['_merge'] == 'left_only'].drop('_merge', axis=1)

    if not df_to_insert.empty:
        df_to_insert.to_sql('dim_time', engine, schema='dw', if_exists='append', index=False)
        print(f"[INCREMENTAL] {len(df_to_insert)} novas datas inseridas.")
    else:
        print("[INCREMENTAL] Nenhuma nova data a inserir.")

# Carga incremental da tabela fato de cervejarias
def incremental_load_fact_breweries():
    parquet_path = '/files/gold/fact_breweries_raw.parquet'
    df_new = pd.read_parquet(parquet_path)
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')
    fact_existing = pd.read_sql('SELECT brewery_name_id, time_id FROM dw.fact_breweries', engine)
    
    # Carregar dimensões para obter surrogate keys
    dim_location = pd.read_sql('SELECT * FROM dw.dim_location', engine)
    dim_type = pd.read_sql('SELECT * FROM dw.dim_brewery_type', engine)
    dim_name = pd.read_sql('SELECT * FROM dw.dim_brewery_name', engine)
    dim_time = pd.read_sql('SELECT * FROM dw.dim_time', engine)

    # Joins para pegar surrogate keys
    df_new = df_new.merge(dim_location, on=['city', 'state', 'country'], how='left')
    df_new = df_new.merge(dim_type, on='brewery_type', how='left')
    df_new = df_new.merge(dim_name, on='api_brewery_id', how='left')
    df_new = df_new.merge(dim_time, on='full_date', how='left')

    fact_df = df_new[[
        'location_id',
        'brewery_type_id',
        'brewery_name_id',
        'time_id',
        'brewery_count',
        'has_website',
        'has_location'
    ]]

    # Considera fato única por (brewery_name_id, time_id)
    to_insert = fact_df.merge(fact_existing, on=['brewery_name_id', 'time_id'], how='left', indicator=True)
    to_insert = to_insert[to_insert['_merge'] == 'left_only'].drop('_merge', axis=1)

    if not to_insert.empty:
        to_insert.to_sql('fact_breweries', engine, schema='dw', if_exists='append', index=False)
        print(f"[INCREMENTAL] {len(to_insert)} novos fatos inseridos.")
    else:
        print("[INCREMENTAL] Nenhum novo fato a inserir.")

# ======================
# DAG de orquestração
# ======================

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='load_dw_breweries_inc',
    schedule_interval='@daily', # Executa diariamente
    catchup=False,
    default_args=default_args,
    description='Carga incremental das dimensões e fato no DW'
) as dag:

    t_load_dim_location = PythonOperator(
        task_id='incremental_load_dim_location',
        python_callable=incremental_load_dim_location
    )
    t_load_dim_type = PythonOperator(
        task_id='incremental_load_dim_brewery_type',
        python_callable=incremental_load_dim_brewery_type
    )
    t_load_dim_name = PythonOperator(
        task_id='incremental_load_dim_brewery_name',
        python_callable=incremental_load_dim_brewery_name
    )
    t_load_fact = PythonOperator(
        task_id='incremental_load_fact_breweries',
        python_callable=incremental_load_fact_breweries
    )

    # Dependências: todas dimensões antes do fato
[t_load_dim_location, t_load_dim_type, t_load_dim_name] >> t_load_fact
