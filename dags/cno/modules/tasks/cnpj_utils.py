# cno/modules/tasks/cnpjutils.py

import shutil
import os
import yaml
import sys
import re

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def move_arquivo(src_file_path, dir_destino):
    """
    Move um arquivo de um diretório para outro.

    :param src_file_path: Caminho completo para o arquivo de origem.
    :param dir_destino: Diretório de destino para onde o arquivo será movido.
    """

    if not os.path.exists(dir_destino):
        os.makedirs(dir_destino)

    file_name = os.path.basename(src_file_path)
    dest_file_path = os.path.join(dir_destino, file_name)

    try:
        shutil.move(src_file_path, dest_file_path)
        print(f"Arquivo {src_file_path} movido para {dest_file_path}")
    except Exception as e:
        print(f"Erro ao mover o arquivo {src_file_path} para {dest_file_path}: {e}")


def le_csv_e_add_cabecalho(file_path, headers, delimiter=";", encoding="iso-8859-1"):
    """
    Lê um arquivo CSV sem cabeçalho e adiciona nomes de colunas.

    :param file_path: Caminho completo para o arquivo CSV.
    :param headers: Lista contendo os nomes das colunas a serem adicionadas.
    :param delimiter: Delimitador usado no arquivo CSV (padrão é ';').
    :param encoding: Codificação do arquivo CSV (padrão é 'iso-8859-1').
    :return: DataFrame com o cabeçalho adicionado.
    """
    try:
        df = pd.read_csv(file_path, header=None, delimiter=delimiter, encoding=encoding)

        if len(headers) != df.shape[1]:
            raise ValueError(
                f"O número de cabeçalhos ({len(headers)}) não corresponde ao \
                    número de colunas ({df.shape[1]}) no arquivo CSV."
            )

        df.columns = headers

        return df
    except Exception as e:
        print(f"Erro ao ler o arquivo {file_path}: {e}")
        return pd.DataFrame()


def aplica_schema(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """
    Aplica o schema ao DataFrame.

    :param df: DataFrame ao qual o schema será aplicado.
    :param schema: Dicionário definindo o schema (nomes das colunas e tipos).
    :return: DataFrame com o schema aplicado.
    """
    try:
        for column, dtype in schema.items():
            if column in df.columns:
                if dtype == "str":
                    df[column] = df[column].astype(str)
                elif dtype == "int":
                    df[column] = pd.to_numeric(
                        df[column], errors="coerce", downcast="integer"
                    )
                elif dtype == "float":
                    df[column] = pd.to_numeric(
                        df[column], errors="coerce", downcast="float"
                    )
                elif dtype == "datetime64[ns]":
                    df[column] = pd.to_datetime(df[column], errors="coerce")
                else:
                    print(
                        f"Tipo de dado desconhecido '{dtype}' \
                            para a coluna '{column}'."
                    )
                    sys.exit(
                        f"Erro: Tipo de dado desconhecido '{dtype}' \
                            para a coluna '{column}'."
                    )

        return df
    except Exception as e:
        sys.exit(f"Erro ao aplicar o schema: {e}")


def filtra_dataframe(df: pd.DataFrame, filtro: dict) -> pd.DataFrame:
    """
    Aplica um filtro no DataFrame baseado nas condições fornecidas.

    :param df: DataFrame ao qual o filtro será aplicado.
    :param filtro: Dicionário contendo as condições de filtragem
    (nome_coluna: valor_desejado).
    :return: DataFrame filtrado com base nas condições fornecidas.
    """
    try:
        df_filtrado = df

        for coluna, valor in filtro.items():
            if coluna in df.columns:
                df_filtrado = df_filtrado[df_filtrado[coluna] == valor]
            else:
                print(f"A coluna '{coluna}' não existe no DataFrame.")

        return df_filtrado
    except Exception as e:
        sys.exit(f"Erro ao aplicar o filtro: {e}")


def salva_parquet(df: pd.DataFrame, output_path: str):
    """
    Salva um DataFrame Pandas em um arquivo Parquet.

    :param df: DataFrame a ser salvo.
    :param output_path: Caminho do arquivo Parquet onde o DataFrame será salvo.
    """
    try:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_path)

        print(f"DataFrame salvo com sucesso em {output_path}")
    except Exception as e:
        sys.exit(f"Erro ao salvar o DataFrame em Parquet: {e}")


def processa_csv(tabela: str, debugging=False) -> pd.DataFrame:
    """
    Processa arquivos CSV adicionando cabeçalho e aplicando o schema.

    :param tabela: Nome da tabela para localizar o arquivo CSV e schema.
    :param debugging: Flag para ativar a impressão de debugging.
    :return: DataFrame processado.
    """
    base_path = "/home/thdamiao/projects/cno/dags/cno/modules/data/"

    with open(os.path.join(base_path, "translate/translate.yaml"), "r") as file:
        data_yaml = yaml.safe_load(file)

    if re.search(r"\d", tabela):
        diretorio = re.sub(r"\d+", "", tabela)
    else:
        diretorio = tabela

    csv_file_path = os.path.join(
        base_path, f"input_files/cnpj/unzip/{diretorio}/{tabela}.csv"
    )
    nome_colunas = data_yaml["cnpj"].get(f"cols_tb_{diretorio}", [])
    schema = data_yaml["cnpj"]["schema"].get(f"tb_{diretorio}", {})
    filtro = data_yaml["cnpj"]["filter"].get(f"tb_{diretorio}", {})

    print(f"Nome colunas: {nome_colunas}")
    print(f"Schema: {schema}")
    print(f"Filtro: {filtro}")

    if not os.path.isfile(csv_file_path):
        raise FileNotFoundError(f"Arquivo CSV não encontrado: {csv_file_path}")

    df = le_csv_e_add_cabecalho(csv_file_path, nome_colunas)
    df = aplica_schema(df, schema)
    df = filtra_dataframe(df, filtro)

    output_path = os.path.join(
        base_path, f"output_files/cnpj/processados_parquet/{diretorio}/{tabela}.parquet"
    )

    salva_parquet(df, output_path)

    move_para = os.path.join(
        base_path, f"input_files/cnpj/arquivos_processados/{diretorio}/"
    )

    move_arquivo(csv_file_path, move_para)

    print(f"Arquivo {tabela} processado!")


def processa_arquivos_em_diretorio(diretorio, debugging=False):
    """
    Processa todos os arquivos CSV em um diretório específico.

    :param diretorio: Nome do diretório contendo arquivos CSV.
    :param debugging: Se True, imprime os DataFrames para depuração.
    """
    base_path = "/home/thdamiao/projects/cno/dags/cno/modules/data/"
    input_dir = os.path.join(base_path, f"input_files/cnpj/unzip/{diretorio}/")

    arquivos = [f for f in os.listdir(input_dir) if f.endswith(".csv")]
    print(f"Arquivos em '{diretorio}': {arquivos}")

    for arquivo in arquivos:
        print(f"Processando arquivo: {arquivo}")
        tabela = os.path.splitext(arquivo)[0]

        # if diretorio in ["empresas", "estabelecimentos", "socios"]:
        #     tabela = diretorio
        processa_csv(tabela, debugging)
