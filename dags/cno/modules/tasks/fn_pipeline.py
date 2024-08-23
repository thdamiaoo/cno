# cno/modules/tasks/fn_pipeline.py

import os
import sys
import pandas as pd
import yaml

from datetime import datetime
from dateutil.relativedelta import relativedelta

# sys.path.insert(0, os.path.abspath("app/dags/"))
sys.path.insert(0, os.path.abspath("dags/"))


from dags.cno.modules.tasks.utils import (
    csv_to_pandas,
    rename_columns,
    create_dataframe,
    save_to_csv,
)

from dags.cno.modules.tasks.cnpj_utils import (
    processa_csv,
    processa_arquivos_em_diretorio,
    carregar_dados_parquet,
)


def run_pipeline_cno(debugging=False):

    # base_path = "/app/dags/cno/modules/data/"
    base_path = "/home/thdamiao/projects/cno/dags/cno/modules/data/"

    with open(base_path + "translate/translate.yaml", "r") as file:
        data_yaml = yaml.safe_load(file)

    # CARREGA YAML
    qualif_resp = data_yaml["cno"]["qualif_resp"]["data_dict"]
    situacao = data_yaml["cno"]["cno_situacao"]["data_dict"]
    vinculos_contrib = data_yaml["cno"]["qualif_contrib"]["data_dict"]

    # CRIA DF A PARTIR DO YAML
    df_qualif_resp = create_dataframe(qualif_resp)
    df_cno_situacao = create_dataframe(situacao)
    df_cno_vin_contrib = create_dataframe(vinculos_contrib)

    dfs = {}
    file_names = data_yaml["cno"]["nome_arquivos"]

    for file_name in file_names:
        # path = os.path.join(base_path + "input_files/cno/", f"{file_name}.csv")
        path = os.path.join(base_path + "input_files/", f"{file_name}.csv")
        print("### File Path:", path)
        df_name = f"df_{file_name}"
        df = csv_to_pandas(path)

        if df is not None:
            df = rename_columns(df)
            dfs[df_name] = df

    for name, df in dfs.items():
        print(f"DataFrame {name} has {len(df)} rows.")
        print(f"Columns after renaming: {df.columns.tolist()}")

    try:
        if "df_cno" in dfs:
            df_cno = dfs["df_cno"].drop_duplicates()
            df_cno = pd.merge(
                df_cno,
                df_qualif_resp,
                on=["qualificacao_do_responsavel"],
                how="left",
            )

            df_cno = pd.merge(df_cno, df_cno_situacao, on=["situacao"], how="left")

            if debugging:
                print("\nExample merge result:")
                print(df_cno.head(10))
                print(df_cno.shape[0])
                print(df_cno.dtypes)

        if "df_cno_areas" in dfs:
            df_cno_areas = dfs["df_cno_areas"].drop_duplicates()
            df_cno = pd.merge(df_cno, df_cno_areas, on=["cno"], how="left")

            if debugging:
                print("\nExample merge result:")
                print(df_cno.head(10))
                print(df_cno.shape[0])
                print(df_cno.dtypes)

        if "df_cno_vinculos" in dfs:
            df_cno_vinculos = dfs["df_cno_vinculos"].drop_duplicates()
            df_cno_vinculos = pd.merge(
                df_cno_vinculos,
                df_cno_vin_contrib,
                on=["qualificacao_do_contribuinte"],
                how="left",
            )

            df_cno = pd.merge(
                df_cno,
                df_cno_vinculos,
                on=[
                    "cno",
                    "data_de_inicio",
                    "data_de_registro",
                    "ni_do_responsavel",
                ],
                how="left",
            )

            if debugging:
                print("\nExample merge result:")
                print(df_cno.head(10))
                print(df_cno.shape[0])
                print(df_cno.dtypes)

        if "df_cno_cnaes" in dfs:
            df_cno_cnaes = dfs["df_cno_cnaes"].drop_duplicates()
            df_cno = pd.merge(
                df_cno,
                df_cno_cnaes,
                on=["cno", "data_de_registro"],
                how="left",
            )

            if debugging:
                print("\nExample merge result:")
                print(df_cno.shape[0])
                print(df_cno.dtypes)

        # REGIÕES BRASIL
        regioes = {
            "sul": data_yaml["cno"]["regiao"]["sul"],
            "sudeste": data_yaml["cno"]["regiao"]["sudeste"],
            "norte": data_yaml["cno"]["regiao"]["norte"],
            "nordeste": data_yaml["cno"]["regiao"]["nordeste"],
            "centro_oeste": data_yaml["cno"]["regiao"]["centro_oeste"],
        }

        data_filtro = (datetime.now() - relativedelta(years=2)).strftime("%Y-%m-%d")
        print(f"Data filtro: {data_filtro}")

        df_cno = df_cno[df_cno["data_de_inicio"] >= data_filtro]

        for regiao, estados in regioes.items():
            df_regiao = df_cno[df_cno["estado"].isin(estados)]

            arquivo_csv = base_path + f"output_files/cno/tbl_fato_cno_{regiao}.csv"

            save_to_csv(df_regiao, arquivo_csv)
            print(f"Arquivo salvo para a região {regiao}: {arquivo_csv}")

    except Exception as e:
        raise ("Erro ao gerar base de CNO", e)


def gera_base_intermediaria_cnpj(debugging=True):
    """
    Executa o pipeline para processar os dados CNPJ.

    :param debugging: Se True, imprime DataFrames para depuração.
    """
    base_path = "/home/thdamiao/projects/cno/dags/cno/modules/data/"
    input_files_path = os.path.join(base_path, "input_files/cnpj/unzip/")
    diretorios = os.listdir(input_files_path)
    # diretorios = ["paises"]

    print(f"Diretórios a processar: {diretorios[0]}")

    for diretorio in diretorios:
        if diretorio in ["empresas", "estabelecimentos", "socios"]:
            print(f"Processando diretório: {diretorio}")
            processa_arquivos_em_diretorio(diretorio, debugging)
        else:
            print(f"Processando arquivo único: {diretorio}")
            processa_csv(diretorio, debugging)


def run_pipeline_cnpj(debugging=False):
    """
    Executa o pipeline para processar os dados CNPJ.

    :param debugging: Se True, o pipeline será executado em modo de depuração.
    """
    base_path = "/home/thdamiao/projects/cno/dags/cno/modules/data/output_files/cnpj/processados_parquet/"

    estabelecimentos_path = os.path.join(base_path, "estabelecimentos")
    print("Carregando dados de estabelecimentos ...")

    df_estabelecimentos = carregar_dados_parquet(estabelecimentos_path)
    
    colunas_estab = [
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "cnae",
        "cnae_secundario",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd",
        "telefone",
        "ddd_2",
        "telefone_2",
        "email",
    ]

    df_estabelecimentos = df_estabelecimentos[colunas_estab]
    print(df_estabelecimentos.head())
