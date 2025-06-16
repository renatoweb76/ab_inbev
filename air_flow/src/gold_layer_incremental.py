import pandas as pd
import os

def generate_gold_files_incremental():
    silver_path = '/files/silver/all_states.parquet'
    gold_path = '/files/gold/'
    os.makedirs(gold_path, exist_ok=True)

    # 1. Ler dados novos da camada Silver
    df_new = pd.read_parquet(silver_path)

    # -------- Dimensão: Localização ----------
    dim_location_file = os.path.join(gold_path, 'dim_location.parquet')
    df_location_new = df_new[['city', 'state', 'country']].drop_duplicates()
    if os.path.exists(dim_location_file):
        df_location_old = pd.read_parquet(dim_location_file)
        df_location = pd.concat([df_location_old, df_location_new]).drop_duplicates()
    else:
        df_location = df_location_new
    df_location.to_parquet(dim_location_file, index=False)

    # -------- Dimensão: Tipo de Cervejaria ----------
    dim_brewery_type_file = os.path.join(gold_path, 'dim_brewery_type.parquet')
    df_type_new = df_new[['brewery_type']].drop_duplicates()
    if os.path.exists(dim_brewery_type_file):
        df_type_old = pd.read_parquet(dim_brewery_type_file)
        df_type = pd.concat([df_type_old, df_type_new]).drop_duplicates()
    else:
        df_type = df_type_new
    df_type.to_parquet(dim_brewery_type_file, index=False)

    # -------- Dimensão: Nome da Cervejaria ----------
    dim_brewery_name_file = os.path.join(gold_path, 'dim_brewery_name.parquet')
    df_name_new = df_new[['id', 'name']].drop_duplicates().rename(
        columns={'id': 'api_brewery_id', 'name': 'brewery_name'}
    )
    if os.path.exists(dim_brewery_name_file):
        df_name_old = pd.read_parquet(dim_brewery_name_file)
        df_name = pd.concat([df_name_old, df_name_new]).drop_duplicates(subset=['api_brewery_id'])
    else:
        df_name = df_name_new
    df_name.to_parquet(dim_brewery_name_file, index=False)

    # -------- Fato: Breweries ----------
    fact_file = os.path.join(gold_path, 'fact_breweries_raw.parquet')
    df_fact_new = df_new[['id', 'name', 'city', 'state', 'country', 'brewery_type',
                          'website_url', 'latitude', 'longitude']].copy()
    df_fact_new['brewery_count'] = 1
    df_fact_new['has_website'] = df_fact_new['website_url'].notna().astype(int)
    df_fact_new['has_location'] = (df_fact_new['latitude'].notna() & df_fact_new['longitude'].notna()).astype(int)
    df_fact_new['full_date'] = current_date
    df_fact_new.rename(columns={'id': 'api_brewery_id', 'name': 'brewery_name'}, inplace=True)
    df_fact_new = df_fact_new[['api_brewery_id', 'brewery_name', 'city', 'state', 'country',
                               'brewery_type', 'full_date', 'brewery_count', 'has_website', 'has_location']]

    if os.path.exists(fact_file):
        df_fact_old = pd.read_parquet(fact_file)
        # Deduplicar por chave de negócio + data
        df_fact = pd.concat([df_fact_old, df_fact_new]).drop_duplicates(subset=['api_brewery_id', 'full_date'])
    else:
        df_fact = df_fact_new
    df_fact.to_parquet(fact_file, index=False)

    print("[GOLD - INCREMENTAL] Arquivos Gold atualizados incrementalmente.")

if __name__ == "__main__":
    generate_gold_files_incremental()
