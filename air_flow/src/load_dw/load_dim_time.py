import pandas as pd
from sqlalchemy import create_engine

# Carrega dados da dimensão tempo
def load_dim_time():
    path = '/files/gold/dim_time.parquet'
    df = pd.read_parquet(path)

    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')
    df.to_sql('dim_time', engine, schema='dw', if_exists='append', index=False)

    print("[LOAD] Carga da dimensão 'dim_time' concluída.")

if __name__ == "__main__":
    load_dim_time()
