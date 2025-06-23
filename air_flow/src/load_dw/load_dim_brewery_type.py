import pandas as pd
from sqlalchemy import create_engine
import glob
import os

def load_dim_brewery_type():
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

    # Concatena todos os DataFrames lidos
    df_total = pd.concat(dataframes, ignore_index=True)
    print(f"[SUCESSO] Total de arquivos lidos: {arquivos_lidos}")
    print(f"[SUCESSO] DataFrame final: {df_total.shape}")
    print(f"[INFO] Arquivos inválidos/ignorados: {len(arquivos_invalidos)}")

   # Monta a dimensão brewery_type
    dim_type = (
        df_total[['brewery_type']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_type['brewery_type_id'] = dim_type.index + 1

    # Conecta ao banco de dados PostgreSQL
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')
    
    # Verifica se a tabela já existe e tenta inserir
    try:
        dim_type.to_sql('dim_brewery_type', engine, schema='dw', if_exists='append', index=False)
        print("[DW] Dimensão brewery_type carregada.")
    except Exception as e:
        print(f"[ERRO FATAL] Não conseguiu inserir: {e}")

if __name__ == "__main__":
    load_dim_brewery_type()
