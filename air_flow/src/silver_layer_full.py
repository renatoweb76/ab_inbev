import pandas as pd
import os

# Ler dados da camada Bronze
def transform_and_partition():
    df = pd.read_json('/files/bronze/breweries_raw.json')

    # Limpa e transforma os dados
    df = df[['id', 'name', 'brewery_type', 'city', 'state', 'country']]
    df.dropna(inplace=True)

    # Salvar particionado por estado
    output_dir = '/files/silver/'
    os.makedirs(output_dir, exist_ok=True)
    for state, group in df.groupby('state'):
        group.to_parquet(f'{output_dir}/state={state}/data.parquet')

    print("[SILVER] Dados transformados e salvos na camada Silver.")

if __name__ == "__main__":
    transform_and_partition()