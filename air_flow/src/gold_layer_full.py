import pandas as pd
import os
import glob

def generate_gold_files():
    silver_path = '/opt/airflow/files/silver/state=*/data.parquet'
    gold_path = '/opt/airflow/files/gold/'
    arquivos = glob.glob(silver_path)
    os.makedirs(gold_path, exist_ok=True)

    # 1. Ler dados da camada Silver
    print("[GOLD] Lendo dados da camada Silver...")
    df = pd.concat([pd.read_parquet(arquivo) for arquivo in arquivos], ignore_index=True)

    # 2. Criar dimensões com surrogate keys

    # Dimensão localização
    dim_location = df[['city', 'state', 'country']].drop_duplicates().reset_index(drop=True)
    dim_location['location_id'] = dim_location.index + 1
    dim_location.to_parquet(os.path.join(gold_path, 'dim_location.parquet'), index=False)

    # Dimensão de tipo de cervejaria
    dim_brewery_type = df[['brewery_type']].drop_duplicates().reset_index(drop=True)
    dim_brewery_type['brewery_type_id'] = dim_brewery_type.index + 1
    dim_brewery_type.to_parquet(os.path.join(gold_path, 'dim_brewery_type.parquet'), index=False)

    # Dimensão de nome da cervejaria (usando id da API como business key)
    dim_brewery_name = df[['id', 'name']].drop_duplicates().reset_index(drop=True)
    dim_brewery_name['brewery_name_id'] = dim_brewery_name.index + 1
    dim_brewery_name = dim_brewery_name.rename(columns={'id': 'api_brewery_id', 'name': 'brewery_name'})
    dim_brewery_name.to_parquet(os.path.join(gold_path, 'dim_brewery_name.parquet'), index=False)

    # 3. Criar tabela fato (com business keys para lookup)
    df_fact = df[['id', 'name', 'city', 'state', 'country', 'brewery_type', 'website_url', 'latitude', 'longitude']].copy()

    # Métricas
    df_fact['brewery_count'] = 1
    df_fact['has_website'] = df_fact['website_url'].notna().astype(int)
    df_fact['has_location'] = (df_fact['latitude'].notna() & df_fact['longitude'].notna()).astype(int)

    # Renomear colunas para padrão da fato
    df_fact.rename(columns={'id': 'api_brewery_id', 'name': 'brewery_name'}, inplace=True)

    # Não precisa de full_date

    # Salvar tabela fato (com business keys, para fazer lookup nas dimensões na etapa de carga DW)
    df_fact = df_fact[['api_brewery_id', 'brewery_name', 'city', 'state', 'country', 'brewery_type',
                       'brewery_count', 'has_website', 'has_location']]

    df_fact.to_parquet(os.path.join(gold_path, 'fact_breweries_raw.parquet'), index=False)

    print("[GOLD] Arquivos da camada Gold foram gerados com sucesso em:", gold_path)

if __name__ == "__main__":
    generate_gold_files()
