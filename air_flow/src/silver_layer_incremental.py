import pandas as pd
import os
import glob

def transform_and_partition_incremental():
    bronze_dir = '/files/bronze/'
    silver_file = '/files/silver/all_states.parquet'
    os.makedirs('/files/silver/', exist_ok=True)

    # 1. Lê todos os arquivos bronze existentes
    bronze_files = glob.glob(os.path.join(bronze_dir, 'bronze_*.parquet'))
    if not bronze_files:
        print("[SILVER - INCREMENTAL] Nenhum arquivo Bronze encontrado.")
        return

    dfs_bronze = [pd.read_parquet(f) for f in bronze_files]
    df_bronze = pd.concat(dfs_bronze, ignore_index=True)

    # 2. (Opcional) Faça as transformações e limpeza da camada Silver aqui
    # Exemplo: manter apenas colunas relevantes
    cols = [
        'id', 'name', 'city', 'state', 'country',
        'brewery_type', 'website_url', 'latitude', 'longitude'
    ]
    df_bronze = df_bronze[cols]

    # 3. Lê o Silver existente (se houver)
    if os.path.exists(silver_file):
        df_silver_old = pd.read_parquet(silver_file)
        # Concatena e deduplica pela business key
        df_silver = pd.concat([df_silver_old, df_bronze]).drop_duplicates(subset=['id'])
    else:
        df_silver = df_bronze.drop_duplicates(subset=['id'])

    # 4. Salva o Silver incremental (um arquivo Parquet único)
    df_silver.to_parquet(silver_file, index=False)
    print(f"[SILVER - INCREMENTAL] Silver atualizado: {silver_file} ({len(df_silver)} registros)")

if __name__ == "__main__":
    transform_and_partition_incremental()
