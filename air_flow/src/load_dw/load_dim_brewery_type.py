import pandas as pd
from sqlalchemy import create_engine

# Carrega dados da dimensão tipo de cervejaria
def load_dim_brewery_type():
    path = '/files/gold/dim_brewery_type.parquet'
    df = pd.read_parquet(path)

    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')
    df.to_sql('dim_brewery_type', engine, schema='dw', if_exists='append', index=False)

    print("[LOAD] Carga da dimensão 'dim_brewery_type' concluída.")

if __name__ == "__main__":
    load_dim_brewery_type()
