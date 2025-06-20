import pandas as pd
from sqlalchemy import create_engine

def load_fact_breweries():
    path = '/opt/airflow/files/gold/fact_breweries_raw.parquet'
    df = pd.read_parquet(path)

    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')

    # Carregar dimensões do DW
    dim_location = pd.read_sql('SELECT * FROM dw.dim_location', engine)
    dim_type = pd.read_sql('SELECT * FROM dw.dim_brewery_type', engine)
    dim_name = pd.read_sql('SELECT * FROM dw.dim_brewery_name', engine)

    # Padronizar tipos e formatação para os joins
    for col in ['city', 'state', 'country']:
        df[col] = df[col].astype(str).str.strip().str.lower()
        dim_location[col] = dim_location[col].astype(str).str.strip().str.lower()
    df['brewery_type'] = df['brewery_type'].astype(str).str.strip().str.lower()
    dim_type['brewery_type'] = dim_type['brewery_type'].astype(str).str.strip().str.lower()

    # Join das dimensões usando as business keys
    df = df.merge(dim_location, on=['city', 'state', 'country'], how='left')
    df = df.merge(dim_type, on='brewery_type', how='left')
    df = df.merge(dim_name, on='api_brewery_id', how='left')

    fact_df = df[[
        'location_id',
        'brewery_type_id',
        'brewery_name_id',
        'brewery_count',
        'has_website',
        'has_location'
    ]]

    # Checagem de FKs nulas
    print(fact_df.isnull().sum())

    fact_df.to_sql('fact_breweries', engine, schema='dw', if_exists='append', index=False)
    print("[LOAD] Fato carregado com sucesso.")

if __name__ == "__main__":
    load_fact_breweries()
