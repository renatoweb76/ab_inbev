import pandas as pd
import os

def transform_and_partition():
    df = pd.read_json('air_flow/files/bronze/breweries_raw.json')

    # Limpa e transforma os dados
    df = df[['id', 'name', 'brewery_type', 'city', 'state', 'country', 'website_url', 'latitude', 'longitude']] 
    df.dropna(inplace=True)

    # Salvar particionado por estado
    output_dir = 'air_flow/files/silver/'
    os.makedirs(output_dir, exist_ok=True)
    for state, group in df.groupby('state'):
        state_dir = f'{output_dir}/state={state}'
        os.makedirs(state_dir, exist_ok=True)
        group.to_parquet(f'{state_dir}/data.parquet')

    print("[SILVER] Dados transformados e salvos na camada Silver.")

if __name__ == "__main__":
    transform_and_partition()