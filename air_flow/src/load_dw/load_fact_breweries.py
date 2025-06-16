import pandas as pd
from sqlalchemy import create_engine

def load_fact_breweries():
    path = '/files/gold/fact_breweries_raw.parquet'
    df = pd.read_parquet(path)

    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')

    # Carregar dimensões do DW
    dim_location = pd.read_sql('SELECT * FROM dw.dim_location', engine)
    dim_type = pd.read_sql('SELECT * FROM dw.dim_brewery_type', engine)
    dim_name = pd.read_sql('SELECT * FROM dw.dim_brewery_name', engine)
    dim_time = pd.read_sql('SELECT time_id, full_date FROM dw.dim_time', engine)  # Agora é o calendário completo

    # Join das dimensões (note que dim_time é só o calendário, não precisa ser gold)
    df = df.merge(dim_location, on=['city', 'state', 'country'], how='left')
    df = df.merge(dim_type, on='brewery_type', how='left')
    df = df.merge(dim_name, on='api_brewery_id', how='left')
    df = df.merge(dim_time, left_on='full_date', right_on='full_date', how='left')

    # Agora, df['time_id'] está certo mesmo que a data não estivesse na gold antes
    fact_df = df[[
        'location_id',
        'brewery_type_id',
        'brewery_name_id',
        'time_id',
        'brewery_count',
        'has_website',
        'has_location'
    ]]

    fact_df.to_sql('fact_breweries', engine, schema='dw', if_exists='append', index=False)
    print("[LOAD] Fato carregado com join correto na dimensão tempo.")

if __name__ == "__main__":
    load_fact_breweries()
