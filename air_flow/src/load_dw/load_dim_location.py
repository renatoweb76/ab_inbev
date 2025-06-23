import pandas as pd
import glob
import os
import re
from sqlalchemy import create_engine

def load_dim_location():
    arquivos = glob.glob('/opt/airflow/files/gold/breweries_dataset/country=*/state=*/*.parquet')
    print(f"[INFO] Encontrados {len(arquivos)} arquivos para processar.")

    dataframes = []
    arquivos_lidos = 0
    arquivos_invalidos = []

    for arquivo in arquivos:
        try:
            if os.path.getsize(arquivo) == 0:
                print(f"[AVISO] Arquivo vazio (ignorado): {arquivo}")
                arquivos_invalidos.append(arquivo)
                continue

            df = pd.read_parquet(arquivo)       
            # Extrai o nome do país e do estado do path do arquivo se não existir no DF
            for col, regex in [('country', r'country=([^/]+)'), ('state', r'state=([^/]+)')]:
                if col not in df.columns:
                    match = re.search(regex, arquivo)
                    if match:
                        df[col] = match.group(1)
                    else:
                        print(f"[WARN] Não achou {col} no path: {arquivo}")
                        df[col] = None  # Pode colocar um valor default

            if df.empty:
                print(f"[AVISO] DataFrame vazio (ignorado): {arquivo}")
                arquivos_invalidos.append(arquivo)
                continue

            dataframes.append(df)
            arquivos_lidos += 1
            print(f"[OK] Arquivo lido: {arquivo} - {df.shape}")
        except Exception as e:
            print(f"[ERRO] Não foi possível ler o arquivo: {arquivo}")
            print(f"       Motivo: {e}")
            arquivos_invalidos.append(arquivo)

    if not dataframes:
        print("[ERRO FATAL] Nenhum arquivo válido foi carregado! Nada a fazer.")
        return

    df_total = pd.concat(dataframes, ignore_index=True)
    print(f"[SUCESSO] Total de arquivos lidos: {arquivos_lidos}")
    print(f"[SUCESSO] DataFrame final: {df_total.shape}")
    print(f"[INFO] Arquivos inválidos/ignorados: {len(arquivos_invalidos)}")

    # Montar a dimensão localização
    if not all(col in df_total.columns for col in ['city', 'state', 'country']):
        print(f"[ERRO FATAL] Faltam colunas em df_total! Colunas disponíveis: {df_total.columns.tolist()}")
        return

    dim_location = (
        df_total[['city', 'state', 'country']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_location['location_id'] = dim_location.index + 1

    # Conecta ao banco de dados PostgreSQL
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')

    # Verifica se a tabela já existe e tenta inserir
    try:
        dim_location.to_sql('dim_location', engine, schema='dw', if_exists='append', index=False)
        print(f"[SUCESSO] Dimensão localização salva no banco de dados (dw.dim_location)")
    except Exception as e:
        print(f"[ERRO FATAL] Não conseguiu inserir: {e}")

if __name__ == "__main__":
    load_dim_location()

