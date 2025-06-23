import pandas as pd
import os
import glob

def transform_and_partition():
    bronze_dir = '/opt/airflow/files/bronze/'
    silver_dir = '/opt/airflow/files/silver/'
    os.makedirs(silver_dir, exist_ok=True)

    bronze_files = glob.glob(os.path.join(bronze_dir, '*.json'))

    # Lista para armazenar DataFrames
    dfs = []
    for f in bronze_files:
        with open(f, 'r', encoding='utf-8', errors='replace') as infile:
            df_temp = pd.read_json(infile)
            dfs.append(df_temp)

    df = pd.concat(dfs, ignore_index=True)

    # Limpeza e transformação
    df = df[['id', 'name', 'brewery_type', 'city', 'state', 'country', 'website_url', 'latitude', 'longitude']]
    df.dropna(subset=['id', 'name', 'brewery_type', 'city', 'state', 'country'], inplace=True)

    # NORMALIZAÇÃO UNICODE — remove caracteres ruins e normaliza acentos
    for col in ['name', 'city', 'state', 'country']:
        df[col] = (
            df[col]
            .astype(str)
            .str.encode('utf-8', errors='replace')
            .str.decode('utf-8')
        )
        
    # Salva em Parquet
    df.to_parquet(os.path.join(silver_dir, 'breweries_silver.parquet'), index=False)

    print("[SILVER] Dados transformados e salvos em silver")

if __name__ == "__main__":
    transform_and_partition()
