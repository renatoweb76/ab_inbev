import pandas as pd
from sqlalchemy import create_engine

def load_dim_time ():
    # Gera calendário de 2000-01-01 até 2030-12-31
    dates = pd.date_range(start="2000-01-01", end="2030-12-31", freq='D')
    df = pd.DataFrame({
        'full_date': dates,
        'year': dates.year,
        'month': dates.month,
        'quarter': dates.quarter,
        'day': dates.day,
        'day_of_week': dates.dayofweek,
        'week_of_year': dates.isocalendar().week
    })
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')
    df.to_sql('dim_time', engine, schema='dw', if_exists='append', index=False)
    print("[LOAD] Dimensão tempo  carregada no DW com trimestre.")

if __name__ == "__main__":
    load_dim_time()
