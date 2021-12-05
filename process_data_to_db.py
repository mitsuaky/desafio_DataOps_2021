import pandas as pd
import numpy as np
import time
from pymongo import MongoClient
from pathlib import Path, PurePath

DATAFRAME_CHUNK_SIZE = 10000

columns = [
    "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial", "nome_fantasia", "situacao_cadastral", "data_situacao_cadastral",
    "motivo_situacao_cadastral", "nome_cidade_no_exterior", "pais", "data_inicio_atividades", "cnae_fiscal_principal",
    "cnae_fiscal_secundaria", "tipo_lagradouro", "lagradouro", "numero", "complemento", "bairro", "cep", "uf", "municipio", "ddd_1",
    "telefone_1", "ddd_2", "telefone_2", "ddd_fax", "fax", "correio_eletronico", "situacao_especial", "data_situacao_especial"]

dtype = "object"


def df_replace(df: pd.DataFrame, column: str, to_replace: dict) -> pd.DataFrame:
    df[column] = df[column].replace(to_replace)
    return df


def df_upper(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    for column in columns:
        df[column] = df[column].str.upper()
    return df


def process_data(df: pd.DataFrame) -> dict:

    df = df.replace({np.nan: None})  # Troca os NaN por Nulo
    df = df_replace(df, "telefone_1", {"0": None})  # Remove os telefones que estão como 0
    df = df_replace(df, "numero", {"SN": "S/N"})  # Padroniza os SN para serem todos S/N
    df = df_upper(df, ["bairro", "complemento", "lagradouro",
                       "nome_cidade_no_exterior", "nome_fantasia"])  # Certifica de deixar tudo maiúsculo

    return df.to_dict('records')


def process_file_to_db(file: PurePath):
    iteration = 0
    total = 0
    # Processa os CSVs por Chunks, para não comer toda a RAM
    # Para evitar o Pandas de tirar os zeros à direita, processamos todos como "object"
    for chunk in pd.read_csv(file, encoding="ISO-8859-1", sep=';', names=columns, dtype=dtype, chunksize=DATAFRAME_CHUNK_SIZE):
        data_dict = process_data(chunk)
        total += len(data_dict)
        collection.insert_many(data_dict)  # Manda pra o MongoDB
        iteration += 1
        print(f"Processando {file.name}...  Iteração: {iteration}, Total: {total}", end='\r')
    return total


def main():
    start = time.time()
    arquivos = [file for file in Path('csv_brutos').glob('*.csv')]
    print(f"> Iniciando processamento de {len(arquivos)} arquivos...")

    total = 0
    finalizados = 0
    for file in Path('csv_brutos').glob('*.csv'):
        file_total = process_file_to_db(file)
        finalizados += 1
        total += file_total
        print(f"> Arquivo {file.name} processado contendo {file_total} estabelecimentos. ({finalizados} de {len(arquivos)})")
    end = time.time()

    print(f"> Processamento finalizado em {(end - start):.2f} segundos. "
          f"Processados {len(arquivos)} arquivos contendo {total} estabelecimentos.")


if __name__ == "__main__":
    client = MongoClient("localhost", 27017, maxPoolSize=10000)
    db = client["dataOps"]
    collection = db["estabelecimentos"]

    main()
