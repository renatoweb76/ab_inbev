import requests
import json
import os

#Obter dados brutos de uma API e salvar em um arquivo JSON
def fetch_raw_data():
    response = requests.get('https://api.openbrewerydb.org/v1/breweries')
    data = response.json()

    os.makedirs('/files/bronze/', exist_ok=True)
    with open('/files/bronze/breweries_raw.json', 'w') as f:
        json.dump(data, f)

    print("[BRONZE] Dados brutos foram salvos em /files/bronze/breweries_raw.json")


if __name__ == "__main__":
    fetch_raw_data()