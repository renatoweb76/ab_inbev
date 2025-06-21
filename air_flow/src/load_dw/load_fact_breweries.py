import pandas as pd
import glob
from sqlalchemy import create_engine

def load_fact_breweries():
    # 1. Conecte ao banco
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')

    # 2. Carregue as dimensões já existentes no DW
    dim_location = pd.read_sql('SELECT location_id, city, state, country FROM dw.dim_location', engine)
    dim_brewery_type = pd.read_sql('SELECT brewery_type_id, brewery_type FROM dw.dim_brewery_type', engine)
    dim_brewery_name = pd.read_sql('SELECT brewery_name_id, name FROM dw.dim_brewery_name', engine)

    # 3. Leia todos os arquivos parquet do staging
    arquivos = glob.glob('/opt/airflow/files/gold/breweries_dataset/country=*/state=*/*.parquet')
    print(f"[INFO] Encontrados {len(arquivos)} arquivos para processar.")

    dataframes = []
    arquivos_invalidos = []

    for arquivo in arquivos:
        try:
            if pd.read_parquet(arquivo).empty:
                print(f"[AVISO] DataFrame vazio (ignorado): {arquivo}")
                arquivos_invalidos.append(arquivo)
                continue
            df = pd.read_parquet(arquivo)
            dataframes.append(df)
            print(f"[OK] Arquivo lido: {arquivo} - {df.shape}")
        except Exception as e:
            print(f"[ERRO] Não foi possível ler o arquivo: {arquivo}")
            print(f"       Motivo: {e}")
            arquivos_invalidos.append(arquivo)

    if not dataframes:
        print("[ERRO FATAL] Nenhum arquivo válido foi carregado! Nada a fazer.")
        return

    # 4. Concatene todos os DataFrames
    df_total = pd.concat(dataframes, ignore_index=True)
    print(f"[SUCESSO] DataFrame final: {df_total.shape}")

    # 5. Normalização das chaves (tudo minúsculo, sem espaço extra)
    for col in ['city', 'state', 'country', 'brewery_type', 'name']:
        if col in df_total.columns:
            df_total[col] = df_total[col].astype(str).str.strip().str.lower()

    for col in ['city', 'state', 'country', 'brewery_type', 'name']:
        if col in dim_location.columns:
            dim_location[col] = dim_location[col].astype(str).str.strip().str.lower()
        if col in dim_brewery_type.columns:
            dim_brewery_type[col] = dim_brewery_type[col].astype(str).str.strip().str.lower()
        if col in dim_brewery_name.columns:
            dim_brewery_name[col] = dim_brewery_name[col].astype(str).str.strip().str.lower()

    # 6. Merge das dimensões (cuidado: SEMPRE left, para não perder registros)
    fact = df_total \
        .merge(dim_location, on=['city', 'state', 'country'], how='left') \
        .merge(dim_brewery_type, on='brewery_type', how='left') \
        .merge(dim_brewery_name, on='name', how='left')

    # 7. Selecionar só as colunas que existem na tabela fato
    fact_breweries = fact[[
        'location_id',
        'brewery_type_id',
        'brewery_name_id',
        'brewery_count',
        'has_website',
        'has_location'
    ]].copy()

    # 8. Verifique se há linhas sem FKs (join mal feito nas dimensões)
    null_rows = fact_breweries[fact_breweries.isnull().any(axis=1)]
    if not null_rows.empty:
        print(f"[ALERTA] Existem {null_rows.shape[0]} linhas sem alguma FK (location, brewery_type ou name). Elas não serão carregadas!")
        fact_breweries = fact_breweries.dropna()

    # 9. Insira na fato
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
