# cno/modules/tasks/utils.py

import os
import sys
import pandas as pd
import unicodedata
import subprocess
import zipfile

# sys.path.insert(0, os.path.abspath("app/dags/"))
sys.path.insert(0, os.path.abspath("/dags"))


def csv_to_pandas(path, encoding="iso-8859-1", sep=",", debugging=False):
    """
    Carrega um arquivo CSV em um DataFrame pandas.

    Parâmetros:
    - path (str): Caminho para o arquivo CSV.
    - encoding (str, opcional): Codificação do arquivo CSV 
      (default: "iso-8859-1").
    - sep (str, opcional): Delimitador utilizado no arquivo CSV (default: ",").
    - debugging (bool, opcional): Se True, imprime as primeiras 10 linhas do 
      DataFrame carregado (default: False).

    Retorna:
    - pandas.DataFrame ou None: DataFrame carregado a partir do arquivo CSV, 
      ou None se ocorrer um erro.
    """

    try:
        df = pd.read_csv(path, sep=sep, encoding=encoding)
        if debugging:
            print(df.head(10))
        return df
    except Exception as e:
        print(f"Error loading {path}: {e}")
        return None


def remove_accents(input_str):
    """
    Remove acentos de caracteres Unicode em uma string.

    Parâmetros:
    - input_str (str): String contendo caracteres com acentos.

    Retorna:
    - str: String sem caracteres acentuados.
    """

    nfkd_form = unicodedata.normalize("NFKD", input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])


def rename_columns(df):
    """
    Renomeia as colunas de um DataFrame pandas para transformá-las em 
    minúsculas e substituir espaços por underscores. Remove também 
    acentos das colunas.

    Parâmetros:
    - df (pandas.DataFrame): O DataFrame pandas cujas colunas serão renomeadas.

    Retorna:
    - pandas.DataFrame: O DataFrame com as colunas renomeadas.
    """

    def clean_column_name(col):
        col = remove_accents(col)
        col = col.lower().replace(" ", "_")
        return col

    df.columns = [clean_column_name(col) for col in df.columns]
    return df


def create_dataframe(data_dict: dict) -> pd.DataFrame:
    """
    Cria um DataFrame pandas a partir de um dicionário de dados.

    Parâmetros:
    - data_dict (dict): Dicionário onde as chaves são os nomes das colunas
                       e os valores são listas de dados para cada coluna.

    Retorna:
    - pandas.DataFrame: DataFrame criado a partir do dicionário de dados.
    """
    return pd.DataFrame(data_dict)


def save_to_csv(df, file_path, sep=",", encoding="utf-8", index=False):
    """
    Salva um DataFrame pandas em um arquivo CSV.

    Parâmetros:
    - df (pandas.DataFrame): DataFrame a ser salvo.
    - file_path (str): Caminho completo onde o arquivo CSV será salvo.
    - sep (str, opcional): Delimitador a ser utilizado no arquivo CSV 
      (default: ',').
    - encoding (str, opcional): Codificação do arquivo CSV (default: 'utf-8').
    - index (bool, opcional): Se True, inclui o índice do DataFrame no arquivo 
      CSV (default: False).
    """
    try:
        df.to_csv(file_path, sep=sep, encoding=encoding, index=index)
        print(f"DataFrame salvo em '{file_path}'")
    except Exception as e:
        print(f"Erro ao salvar DataFrame em '{file_path}': {e}")


def download_and_extract_zip(output_dir, url):
    """
    Faz o download e extrai o arquivo ZIP do Cadastro Nacional de Obras (CNO)
    da Receita Federal Brasileira.

    Parâmetros:
    - output_dir (str): Diretório onde o arquivo ZIP será salvo e
    descompactado.
    """

    os.makedirs(output_dir, exist_ok=True)

    file_name = url.split("/")[-1]
    print(f"Nome do arquivo: {file_name}")
    zip_file_path = os.path.join(output_dir, file_name)

    try:
        # Download do arquivo ZIP
        subprocess.run(["wget", url, "-O", zip_file_path])
        print(f"Download concluído: {file_name}")

        # Extrair o arquivo ZIP
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(output_dir)

        print(f"Arquivo ZIP extraído em: {output_dir}")
    except Exception as e:
        print(f"Erro durante o download ou extração de {url}: {e}")
