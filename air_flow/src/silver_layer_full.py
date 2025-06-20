import pandas as pd
import os
import glob

def transform_and_partition():
    bronze_dir = '/opt/airflow/files/bronze/'
    silver_dir = '/opt/airflow/files/silver/'
    os.makedirs(silver_dir, exist_ok=True)

    # Lê todos os JSONs da bronze (ajuste o padrão conforme seus arquivos)
    bronze_files = glob.glob(os.path.join(bronze_dir, '*.json'))
    df = pd.concat([pd.read_json(f) for f in bronze_files], ignore_index=True)

    # Limpeza e transformação (ajuste as colunas conforme seu modelo)
    df = df[['id', 'name', 'brewery_type', 'city', 'state', 'country', 'website_url', 'latitude', 'longitude']]
    df.dropna(subset=['id', 'name', 'brewery_type', 'city', 'state', 'country'], inplace=True)

    # Salva TUDO em um único Parquet
    df.to_parquet(os.path.join(silver_dir, 'breweries_silver.parquet'), index=False)

    print("[SILVER] Dados transformados e salvos em silver")

if __name__ == "__main__":
    transform_and_partition()
