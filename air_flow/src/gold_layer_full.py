import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

def generate_gold_files():
    silver_path = '/opt/airflow/files/silver/breweries_silver.parquet'
    gold_path = '/opt/airflow/files/gold/fact_breweries_dataset/'
    os.makedirs(gold_path, exist_ok=True)

    print("[GOLD] Lendo dados da camada Silver...")
    df = pd.read_parquet(silver_path)

    # Aqui você pode gerar colunas extras ou fazer joins/deduplicações se quiser.
    # Exemplo de métricas:
    df['brewery_count'] = 1
    df['has_website'] = df['website_url'].notna().astype(int)
    df['has_location'] = (df['latitude'].notna() & df['longitude'].notna()).astype(int)

    # Agora escrevemos tudo como um único dataset Parquet particionado:
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
