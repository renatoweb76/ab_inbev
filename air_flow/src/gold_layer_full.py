import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import unicodedata
import re

# Função para limpar nomes de países e estados
def clean_name(name):
    if pd.isnull(name):
        return "unknown"
    # Remove acentos
    nfkd = unicodedata.normalize('NFKD', str(name))
    name = u"".join([c for c in nfkd if not unicodedata.combining(c)])
    # Substitui espaços e caracteres não alfanuméricos por "_"
    name = re.sub(r'[^a-zA-Z0-9]', '_', name)
    # Remove múltiplos underscores
    name = re.sub(r'_+', '_', name)
    # Remove underscores do início/fim
    return name.strip('_')

def generate_gold_files():

    silver_path = '/opt/airflow/files/silver/breweries_silver.parquet'
    gold_path = '/opt/airflow/files/gold/breweries_dataset/'

    #silver_path = '/opt/airflow/files/silver/breweries_silver.parquet'
    #gold_path = '/opt/airflow/files/gold/breweries_dataset/'
    os.makedirs(gold_path, exist_ok=True)

    print("[GOLD] Lendo dados da camada Silver...")
    df = pd.read_parquet(silver_path)

    # Garante que as colunas existem e preenche nulos
    for col in ['country', 'state']:
        if col not in df.columns:
            df[col] = "unknown"
        df[col] = df[col].fillna("unknown")

    # Colunas limpas para particionamento
    df['country'] = df['country'].map(clean_name)
    df['state'] = df['state'].map(clean_name)

    # Métricas
    df['brewery_count'] = 1
    df['has_website'] = df['website_url'].notna().astype(int)
    df['has_location'] = (df['latitude'].notna() & df['longitude'].notna()).astype(int)

    # Garante que country/state estão presentes e nunca nulos antes de salvar
    assert not df['country'].isnull().any(), "Campo 'country' com nulos"
    assert not df['state'].isnull().any(), "Campo 'state' com nulos"
  
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=gold_path,
        partition_cols=['country', 'state'],
        existing_data_behavior='overwrite_or_ignore'
    )

    print(f"[GOLD] Dataset particionado gerado em: {gold_path}")

if __name__ == "__main__":
    generate_gold_files()
