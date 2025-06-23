import pandas as pd
import glob
import os
import re
from sqlalchemy import create_engine

def extract_partition_values(filepath):
    """Extrai os valores de country e state do caminho do arquivo Parquet particionado."""
    match = re.search(r'country=([^/]+)/state=([^/]+)/', filepath)
    if match:
        return match.group(1), match.group(2)
    else:
        return None, None

def load_fact_breweries():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')

    dim_location = pd.read_sql('SELECT location_id, city, state, country FROM dw.dim_location', engine)
    dim_brewery_type = pd.read_sql('SELECT brewery_type_id, brewery_type FROM dw.dim_brewery_type', engine)
    dim_brewery_name = pd.read_sql('SELECT brewery_name_id, brewery_name FROM dw.dim_brewery_name', engine)

    arquivos = glob.glob('/opt/airflow/files/gold/breweries_dataset/country=*/state=*/*.parquet')
    print(f"[INFO] Encontrados {len(arquivos)} arquivos para processar.")

    dataframes = []
    arquivos_invalidos = []

    for arquivo in arquivos:
        try:
            df = pd.read_parquet(arquivo)
            country, state = extract_partition_values(arquivo)
            if 'country' not in df.columns or df['country'].isnull().all():
                df['country'] = country
            if 'state' not in df.columns or df['state'].isnull().all():
                df['state'] = state

            obrigatorias = ['id', 'name', 'brewery_type', 'city', 'state', 'country']
            faltando = [col for col in obrigatorias if col not in df.columns]
            if faltando:
                print(f"[ERRO] Arquivo {arquivo} não tem as colunas obrigatórias: {faltando} (tem: {df.columns.tolist()})")
                arquivos_invalidos.append(arquivo)
                continue

            dataframes.append(df)
            print(f"[OK] Arquivo lido: {arquivo} - {df.shape}")
        except Exception as e:
            print(f"[ERRO] Não foi possível ler o arquivo: {arquivo}")
            print(f"       Motivo: {e}")
            arquivos_invalidos.append(arquivo)

    if not dataframes:
        print("[ERRO FATAL] Nenhum arquivo válido foi carregado! Nada a fazer.")
        return

    df_total = pd.concat(dataframes, ignore_index=True)
    print(f"[SUCESSO] DataFrame final: {df_total.shape}")
    print(f"[DEBUG] Colunas df_total: {df_total.columns.tolist()}")
    print(f"[DEBUG] Registros únicos brewery_name/city/state/country na gold: {df_total[['name', 'city', 'state', 'country']].drop_duplicates().shape}")

    # Normalização
    for col in ['city', 'state', 'country', 'brewery_type', 'name']:
        if col in df_total.columns:
            df_total[col] = (
                df_total[col]
                .astype(str)
                .str.encode('utf-8', errors='replace')
                .str.decode('utf-8')
                .str.strip()
                .str.lower()
            )

    # Renomeia 'name' para 'brewery_name'
    if 'name' in df_total.columns and 'brewery_name' not in df_total.columns:
        df_total = df_total.rename(columns={'name': 'brewery_name'})

    # Padroniza as dimensões para join
    for col in ['city', 'state', 'country']:
        if col in dim_location.columns:
            dim_location[col] = dim_location[col].astype(str).str.strip().str.lower()
    if 'brewery_type' in dim_brewery_type.columns:
        dim_brewery_type['brewery_type'] = dim_brewery_type['brewery_type'].astype(str).str.strip().str.lower()
    if 'brewery_name' in dim_brewery_name.columns:
        dim_brewery_name['brewery_name'] = dim_brewery_name['brewery_name'].astype(str).str.strip().str.lower()

    # ========= CHECAGEM DE DUPLICIDADE NAS DIMENSÕES =========
    print("\n[DEBUG] Checagem de duplicidades nas dimensões:")
    for dim_df, subset, nome in [
        (dim_location, ['city', 'state', 'country'], 'dim_location'),
        (dim_brewery_type, ['brewery_type'], 'dim_brewery_type'),
        (dim_brewery_name, ['brewery_name'], 'dim_brewery_name')
    ]:
        dups = dim_df[dim_df.duplicated(subset=subset, keep=False)]
        print(f"[{nome}] Total: {dim_df.shape[0]}, Únicos: {dim_df.drop_duplicates(subset=subset).shape[0]}")
        if not dups.empty:
            print(f"[ERRO GRAVE] Duplicidade na dimensão {nome} para chaves {subset}:")
            print(dups)
        else:
            print(f"[OK] Dimensão {nome} sem duplicidade nas chaves {subset}")

    # ========= MERGE =========
    fact = df_total \
        .merge(dim_location, on=['city', 'state', 'country'], how='left') \
        .merge(dim_brewery_type, on='brewery_type', how='left') \
        .merge(dim_brewery_name, on='brewery_name', how='left')

    print(f"[DEBUG] Linhas pós-merge: {fact.shape}")
    print(f"[DEBUG] Registros únicos brewery_name/city/state/country após merge: {fact[['brewery_name', 'city', 'state', 'country']].drop_duplicates().shape}")

    # ========== DEDUPLICAÇÃO NA FATO ==========
    antes = fact.shape[0]
    fact = fact.drop_duplicates(subset=['brewery_name', 'city', 'state', 'country'])
    depois = fact.shape[0]
    print(f"[FATO][DEDUP] Linhas antes: {antes}, após dedup: {depois}")

    # Campos extras da fato
    if 'brewery_count' not in fact.columns:
        fact['brewery_count'] = 1
    if 'has_website' not in fact.columns:
        fact['has_website'] = fact['website_url'].apply(lambda x: 1 if pd.notnull(x) and str(x).strip() != '' else 0)
    if 'has_location' not in fact.columns:
        fact['has_location'] = fact.apply(
            lambda row: 1 if pd.notnull(row.get('latitude')) and pd.notnull(row.get('longitude')) else 0, axis=1
        )

    fact_breweries = fact[[
        'location_id',
        'brewery_type_id',
        'brewery_name_id',
        'brewery_count',
        'has_website',
        'has_location'
    ]].copy()

    null_rows = fact_breweries[fact_breweries.isnull().any(axis=1)]
    if not null_rows.empty:
        print(f"[ALERTA] {null_rows.shape[0]} linhas com FK nula serão descartadas.")
        fact_breweries = fact_breweries.dropna()

    try:
        fact_breweries = fact_breweries.astype({
            'location_id': int,
            'brewery_type_id': int,
            'brewery_name_id': int,
            'brewery_count': int,
            'has_website': int,
            'has_location': int
        })
        fact_breweries.to_sql('fact_breweries', engine, schema='dw', if_exists='append', index=False)
        print(f"[SUCESSO] Fato inserida: {fact_breweries.shape[0]} linhas")
    except Exception as e:
        print(f"[ERRO FATAL] Não conseguiu inserir: {e}")

if __name__ == "__main__":
    load_fact_breweries()
