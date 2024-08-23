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

from dags.cno.modules.tasks.cnpj_utils import processa_csv


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


# def run_pipeline_cnpj(debugging=False):
#     # base_path = "/app/dags/cno/modules/data/"
#     base_path = "/home/thdamiao/projects/cno/dags/cno/modules/data/"

#     with open(base_path + "translate/translate.yaml", "r") as file:
#         data_yaml = yaml.safe_load(file)

#     try:
#         # # TBS EMPRESAS
#         # cols_tb_empresas = data_yaml["cnpj"]["cols_tb_empresas"]
#         # diretorio_empresas = os.path.join(base_path, "input_files/cnpj/unzip/empresas/")
#         # df_empresas = tabelas_para_df(diretorio_empresas, cols_tb_empresas)

#         # if debugging:
#         #     df_empresas.info()

#         # TBS ESTABELECIMENTOS
#         cols_tb_estabelecimentos = data_yaml["cnpj"]["cols_tb_estabelecimentos"]
#         diretorio_estabelecimentos = os.path.join(
#             base_path, "input_files/cnpj/unzip/estabelecimentos/"
#         )
#         df_estabelecimentos = tabelas_para_df(
#             diretorio_estabelecimentos, cols_tb_estabelecimentos
#         )

#         if debugging:
#             df_estabelecimentos.info()
#             print(df_estabelecimentos.head())

#         # # TBL SOCIOS
#         # cols_tb_socios = data_yaml["cnpj"]["cols_tb_socios"]
#         # diretorio_socios = os.path.join(base_path, "input_files/cnpj/unzip/socios/")
#         # df_socios = tabelas_para_df(diretorio_socios, cols_tb_socios)

#         # if debugging:
#         #     df_socios.info()

#         print(
#             "Dados das tabelas de Empresas, Estabelecimentos"
#             + "e sócios carregados em pandas"
#         )

#     except Exception as e:
#         print(f"Erro na carga das tabelas de Empresas: {e}")

#     # print("Inciando construção de tabela FATO")

#     # df_cnpj = df_estabelecimentos.merge(df_socios, on="cnpj_basico", how="left")

#     # if debugging:
#     #     df_cnpj.info()
#     #     print(df_cnpj.head())


def run_pipeline_cnpj(debugging=True):
    processa_csv("socios")
