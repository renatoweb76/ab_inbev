import pandas as pd
import os
from datetime import datetime
import requests

def fetch_raw_data_incremental():
    bronze_dir = '/opt/airflow/files/bronze/'
    os.makedirs(bronze_dir, exist_ok=True)

    # 1. Definir a data do batch
    today_str = datetime.today().strftime('%Y-%m-%d')
    bronze_file = os.path.join(bronze_dir, f'bronze_{today_str}.parquet')

    # 2. Consultar
    api_url = "https://api.openbrewerydb.org/breweries?per_page=100"
    response = requests.get(api_url)
    if response.status_code != 200:
        raise Exception(f"Erro ao acessar a API: {response.status_code}")

    data = response.json()
    df_new = pd.DataFrame(data)
    if df_new.empty:
        print("[BRONZE - INCREMENTAL] Nenhum dado novo retornado pela API.")
        return

    # 3. Salva o batch como novo arquivo Parquet (append natural)
    df_new.to_parquet(bronze_file, index=False)
    print(f"[BRONZE - INCREMENTAL] Batch salvo: {bronze_file} ({len(df_new)} registros)")

if __name__ == "__main__":
    fetch_raw_data_incremental()