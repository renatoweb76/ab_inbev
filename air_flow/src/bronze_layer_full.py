import requests
import json
import os

#Obter dados brutos de uma API e salvar em um arquivo JSON
def fetch_raw_data():
    try:
        response = requests.get('https://api.openbrewerydb.org/v1/breweries')
        data = response.json()

        os.makedirs('air_flow/files/bronze/', exist_ok=True)
        with open('air_flow/files/bronze/breweries_raw.json', 'w') as f:json.dump(data, f)

        print("[BRONZE] Dados brutos foram salvos em /files/bronze/breweries_raw.json")
    except Exception as e:
        print(f"[ERRO] Falha ao buscar dados brutos: {e}")


if __name__ == "__main__":
    fetch_raw_data()