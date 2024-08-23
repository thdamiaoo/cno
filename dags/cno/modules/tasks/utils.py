# cno/modules/tasks/utils.py

import os
import sys
import pandas as pd
import unicodedata
import subprocess
import zipfile
import pyarrow.parquet as pq
import pyarrow as pa
import yaml

from sqlalchemy import create_engine

DATABASE_URL = "postgresql://user:password@db:5432/cno"
NOME_TABELA = "tb_fato_cno"

engine = create_engine(DATABASE_URL)

# sys.path.insert(0, os.path.abspath("app/dags/"))
sys.path.insert(0, os.path.abspath("/dags"))


# def csv_to_pandas(
#     path, encoding="iso-8859-1", sep=",", header=None, names="", debugging=False
# ):
#     """
#     Carrega um arquivo CSV em um DataFrame pandas.

#     Parâmetros:
#     - path (str): Caminho para o arquivo CSV.
#     - encoding (str, opcional): Codificação do arquivo CSV
#       (default: "iso-8859-1").
#     - sep (str, opcional): Delimitador utilizado no arquivo CSV (default: ",").
#     - debugging (bool, opcional): Se True, imprime as primeiras 10 linhas do
#       DataFrame carregado (default: False).

#     Retorna:
#     - pandas.DataFrame ou None: DataFrame carregado a partir do arquivo CSV,
#       ou None se ocorrer um erro.
#     """

#     try:
#         df = pd.read_csv(path, encoding=encoding, sep=sep, header=header, names=names)
#         if debugging:
#             print(df.head(10))
#         return df
#     except Exception as e:
#         print(f"Error loading {path}: {e}")
#         return None


def csv_to_pandas(caminho, encoding="utf-8", sep=",", names=None, dtype=None):
    try:
        df = pd.read_csv(
            caminho,
            encoding=encoding,
            sep=sep,
            header=None if names else "infer",
            names=names,
            dtype=dtype,
            low_memory=False,
        )
        return df
    except pd.errors.ParserError as e:
        print(f"Erro ao analisar o arquivo {caminho}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Erro ao ler o arquivo {caminho}: {e}")
        return pd.DataFrame()


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


def import_csv_to_db(csv_path, table_name):
    """
    Importa dados de um arquivo CSV para uma tabela no banco de dados
    PostgreSQL.
    Esta função lê os dados de um arquivo CSV localizado em `csv_path` e os
    importa para a tabela especificada em `table_name` no banco de dados
    PostgreSQL conectado através da `engine` SQLAlchemy.
    Parâmetros:
    csv_path (str): O caminho para o arquivo CSV a ser importado.
    table_name (str): O nome da tabela no banco de dados onde os dados
    serão importados.
    Exceções:
    - Se ocorrer um erro durante a leitura do CSV ou a importação dos dados,
      uma mensagem de erro será impressa.
    Exemplo:
    >>> import_csv_to_db('/shared/file1.csv', 'table1')
    Dados importados para a tabela table1 com sucesso!
    """
    try:
        df = pd.read_csv(csv_path)
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"Dados importados para a tabela {table_name} com sucesso!")
    except Exception as e:
        print(f"Erro durante a importação da tabela {table_name} para o DB: {e}")


def tabelas_para_df(diretorio, cols, encoding="iso-8859-1", sep=";"):
    try:
        print(f"Carregando dados do diretório {diretorio}..")
        files = os.listdir(diretorio)

        if not files:
            print("Nenhum arquivo encontrado no diretório especificado.")
            return pd.DataFrame()

        dfs = []

        for file in files:
            path = os.path.join(diretorio, file)

            if not file.endswith(".csv"):
                print(f"Arquivo {file} não é um CSV e será ignorado.")
                continue

            df = csv_to_pandas(
                path,
                encoding=encoding,
                sep=sep,
                names=cols,
            )
            if not df.empty:
                dfs.append(df)
                print(f"Arquivo: {file}")
            else:
                print(f"DataFrame vazio retornado para o arquivo {file}.")

        if dfs:
            df_final = pd.concat(dfs, ignore_index=True)
            return df_final
        else:
            print("Nenhum dado foi carregado.")
            return pd.DataFrame()

    except Exception as e:
        print(f"Erro na carga dos dados: {e}")
        return pd.DataFrame()


def save_csv_as_parquet(input_dir, output_dir, encoding="iso-8859-1", sep=";"):
    """
    Converte todos os arquivos CSV em um diretório para o formato Parquet e
    os salva em um diretório de saída.

    Parâmetros:
    ----------
    input_dir : str
        Caminho para o diretório que contém os arquivos CSV a serem
        convertidos.
    output_dir : str
        Caminho para o diretório onde os arquivos Parquet serão salvos.
    encoding : str, opcional
        Encoding utilizado para ler os arquivos CSV. O padrão é "iso-8859-1".
    sep : str, opcional
        Delimitador utilizado nos arquivos CSV. O padrão é ";".

    Retorno:
    -------
    None
        A função salva os arquivos Parquet no diretório de saída especificado.
    """

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    files = [f for f in os.listdir(input_dir) if f.endswith(".csv")]

    for file in files:
        csv_path = os.path.join(input_dir, file)
        print(f"Lendo {file}...")

        df = pd.read_csv(csv_path, encoding=encoding, sep=sep)

        # Converter colunas com tipos mistos para numéricos, ignorando erros
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        parquet_path = os.path.join(output_dir, file.replace(".csv", ".parquet"))
        df.to_parquet(parquet_path)
        print(f"Salvo {file} como {parquet_path}")

    print("Todos os arquivos foram processados.")


# def load_parquet_files(input_dir):
#     """
#     Lê todos os arquivos Parquet de um diretório e os carrega em um único DataFrame do Pandas.

#     Parâmetros:
#     ----------
#     input_dir : str
#         Caminho para o diretório que contém os arquivos Parquet a serem carregados.

#     Retorno:
#     -------
#     pd.DataFrame
#         Um DataFrame contendo os dados de todos os arquivos Parquet combinados.
#     """

#     # Listar todos os arquivos .parquet no diretório
#     files = [f for f in os.listdir(input_dir) if f.endswith('.parquet')]

#     if not files:
#         print("Nenhum arquivo Parquet encontrado no diretório especificado.")
#         return pd.DataFrame()

#     # Lista para armazenar DataFrames carregados
#     dfs = []

#     # Iterar sobre os arquivos e carregá-los em DataFrames
#     for file in files:
#         parquet_path = os.path.join(input_dir, file)
#         print(f"Lendo {file}...")
#         df = pd.read_parquet(parquet_path)
#         dfs.append(df)

#     # Concatenar todos os DataFrames em um único DataFrame
#     if dfs:
#         df_final = pd.concat(dfs, ignore_index=True)
#         return df_final
#     else:
#         print("Nenhum dado foi carregado.")
#         return pd.DataFrame()


def load_parquet_files_with_columns(input_dir, column_names):
    """
    Lê todos os arquivos Parquet de um diretório e os carrega em um único
    DataFrame do Pandas,
    adicionando nomes de colunas personalizados.

    Parâmetros:
    ----------
    input_dir : str
        Caminho para o diretório que contém os arquivos Parquet a serem
        carregados.
    column_names : list
        Lista com os nomes das colunas na ordem correta.

    Retorno:
    -------
    pd.DataFrame
        Um DataFrame contendo os dados de todos os arquivos Parquet combinados,
        com os nomes das colunas definidos.
    """

    # Listar todos os arquivos .parquet no diretório
    files = [f for f in os.listdir(input_dir) if f.endswith(".parquet")]

    if not files:
        print("Nenhum arquivo Parquet encontrado no diretório especificado.")
        return pd.DataFrame()

    # Lista para armazenar DataFrames carregados
    dfs = []

    # Iterar sobre os arquivos e carregá-los em DataFrames
    for file in files:
        parquet_path = os.path.join(input_dir, file)
        print(f"Lendo: {file}...")
        df = pd.read_parquet(parquet_path)

        # Adicionar os nomes das colunas
        df.columns = column_names

        dfs.append(df)

    # Concatenar todos os DataFrames em um único DataFrame
    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        return df_final
    else:
        print("Nenhum dado foi carregado.")
        return pd.DataFrame()


def add_header_and_load_csv(arquivo, cabecalho, encoding="utf-8", sep=";"):
    """
    Carrega um arquivo CSV sem cabeçalho e adiciona um cabeçalho.

    :param file_path: Caminho para o arquivo CSV.
    :param header: Lista de nomes de colunas para adicionar.
    :return: DataFrame com o cabeçalho adicionado.
    """
    print("Path arquivo:", arquivo)
    try:
        df = pd.read_csv(
            arquivo,
            encoding=encoding,
            sep=sep,
            # header=None if names else "infer",
            # names=header,
            # dtype=dtype,
            low_memory=False,
        )

        return df
    except pd.errors.ParserError as e:
        print(f"Erro ao analisar o arquivo {arquivo}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Erro ao ler o arquivo {arquivo}: {e}")
        return pd.DataFrame()


def apply_schema_and_filters(df, schema, filters=None):
    """
    Aplica o schema e filtros no DataFrame.

    :param df: DataFrame a ser transformado.
    :param schema: Dicionário definindo o schema (nomes das colunas e tipos).
    :param filters: Lista de filtros a serem aplicados.
    :return: DataFrame com schema e filtros aplicados.
    """
    # Aplicar schema
    df = df.astype(schema)

    # Aplicar filtros
    if filters:
        for column, condition in filters.items():
            df = df.query(f"{column} {condition}")

    return df


def save_to_parquet(df, output_path):
    """
    Salva um DataFrame em formato Parquet.

    :param df: DataFrame a ser salvo.
    :param output_path: Caminho para o arquivo Parquet de saída.
    """
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_path)


def process_csv_files_in_directory(input_dir, output_dir, header, schema, filters=None):
    """
    Processa todos os arquivos CSV em um diretório e salva os DataFrames
    resultantes em Parquet.

    :param input_dir: Diretório de entrada contendo arquivos CSV.
    :param output_dir: Diretório de saída para arquivos Parquet.
    :param header: Lista de nomes de colunas para adicionar.
    :param schema: Dicionário definindo o schema (nomes das colunas e tipos).
    :param filters: Lista de filtros a serem aplicados (opcional).
    """

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for file_name in os.listdir(input_dir):
        if file_name.endswith(".csv"):
            # input_file_path = os.path.join(input_dir, file_name)
            output_file_path = os.path.join(
                output_dir, file_name.replace(".csv", ".parquet")
            )

            # Adicionar cabeçalho e carregar os dados
            # df = add_header_and_load_csv(input_file_path, header)
            df = tabelas_para_df(input_dir, cols=header)
            # print(df.head(10))
            
            # sys.exit()
            # Aplicar schema e filtros
            df = apply_schema_and_filters(df, schema, filters)

            # Salvar o DataFrame em formato Parquet
            save_to_parquet(df, output_file_path)
            print(f"Arquivo {file_name} processado e salvo em {output_file_path}")


def carrega_parquet():

    base_path = "/home/thdamiao/projects/cno/dags/cno/modules/data/"

    with open(base_path + "translate/translate.yaml", "r") as file:
        data_yaml = yaml.safe_load(file)

    dirs = ["empresas", "estabelecimentos", "socios"]

    for dir_name in dirs:
        input_dir = os.path.join(base_path, f"input_files/cnpj/unzip/{dir_name}")
        output_dir = os.path.join(
            base_path, f"output_files/cnpj/cnpj_filter/{dir_name}"
        )

        print(f"Carregando dados da tabela: {dir_name}")

        header = data_yaml["cnpj"].get(f"cols_tb_{dir_name}", [])
        schema = data_yaml["cnpj"]["schema"].get(f"cols_tb_{dir_name}", {})
        filter = data_yaml["cnpj"]["filter"].get(f"cols_tb_{dir_name}", None)

        print(f"Colunas tabela {dir_name}: {header}")
        print(f"Schema da tabela {dir_name}: {schema}")
        print(f"Filtro tabela {dir_name}: {filter}")

        process_csv_files_in_directory(input_dir, output_dir, header, schema, filter)
