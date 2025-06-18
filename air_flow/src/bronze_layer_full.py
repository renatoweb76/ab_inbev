import requests
import json
import os
import time

def fetch_raw_data():
    try:
        all_breweries = []
        base_url = "https://api.openbrewerydb.org/v1/breweries"
        page = 1
        per_page = 200

        while True:
            print(f"[BRONZE] Baixando página {page}...")
            response = requests.get(base_url, params={"page": page, "per_page": per_page})
            response.raise_for_status()
            data = response.json()
            if not data:
                break
            all_breweries.extend(data)
            if len(data) < per_page:
                break  # Chegou ao final
            page += 1
            time.sleep(0.1)  # delay para evitar sobrecarga no servidor

        os.makedirs('/opt/airflow/files/bronze/', exist_ok=True)
        with open('/opt/airflow/files/bronze/breweries_raw.json', 'w') as f:
            json.dump(all_breweries, f, indent=2)

        print(f"[BRONZE] Total de cervejarias baixadas: {len(all_breweries)}")
        print("[BRONZE] Dados brutos foram salvos em /opt/airflow/files/bronze/breweries_raw.json")
    except Exception as e:
        print(f"[ERRO] Falha ao buscar dados brutos: {e}")

if __name__ == "__main__":
    fetch_raw_data()
