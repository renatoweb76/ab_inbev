import requests
import json
import os
import time
import unicodedata

def normalize_text(text):
    if not isinstance(text, str):
        return text
    try:
        # Remove acentos/caracteres não-ASCII (defesa básica)
        nfkd = unicodedata.normalize('NFKD', text)
        return ''.join([c for c in nfkd if not unicodedata.combining(c)])
    except Exception:
        return text

def normalize_record(record):
    # Normaliza cada campo string do registro (dict)
    return {k: normalize_text(v) if isinstance(v, str) else v for k, v in record.items()}

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
            # Normaliza cada registro recebido
            data = [normalize_record(rec) for rec in data]
            all_breweries.extend(data)
            if len(data) < per_page:
                break
            page += 1
            time.sleep(0.1)

        os.makedirs('/opt/airflow/files/bronze/', exist_ok=True)
        with open('/opt/airflow/files/bronze/breweries_raw.json', 'w', encoding='utf-8') as f:
            json.dump(all_breweries, f, indent=2, ensure_ascii=False)

        print(f"[BRONZE] Total de cervejarias baixadas: {len(all_breweries)}")
        print("[BRONZE] Dados brutos normalizados e salvos em /opt/airflow/files/bronze/breweries_raw.json")
    except Exception as e:
        print(f"[ERRO] Falha ao buscar dados brutos: {e}")

if __name__ == "__main__":
    fetch_raw_data()
