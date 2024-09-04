# cno/modules/tasks/fn_pipeline.py

import os
import sys
import pandas as pd
import yaml
import gc

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
    aplica_formatacao,
    salva_parquet,
)

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

# base_path = "/app/dags/cno/modules/data/" TO_COMPOSE
base_path = "/home/thdamiao/projects/cno/dags/cno/modules/data/"

with open(base_path + "translate/translate.yaml", "r") as file:
    data_yaml = yaml.safe_load(file)


def run_pipeline_cno(debugging=False):

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


# ATENÇÃO
def gera_base_intermediaria_cnpj(debugging=True):
    """
    Executa o pipeline para processar os dados CNPJ.

    :param debugging: Se True, imprime DataFrames para depuração.
    """

    input_files_path = os.path.join(base_path, "input_files/cnpj/unzip/")
    diretorios = os.listdir(input_files_path)
    # diretorios = ["empresas"]

    print(f"Diretórios a processar: {diretorios[0]}")

    for diretorio in diretorios:
        if diretorio in ["empresas", "estabelecimentos", "socios"]:
            print(f"Processando diretório: {diretorio}")
            processa_arquivos_em_diretorio(diretorio, debugging)
        else:
            print(f"Processando arquivo único: {diretorio}")
            processa_csv(diretorio, debugging)


def gera_df_cno_estabelecimentos(debugging=False):
    """
    Executa o pipeline para processar os dados CNPJ.

    :param debugging: Se True, o pipeline será executado em modo de depuração.
    """
    nova_base_path = os.path.join(base_path, "output_files/cnpj/processados_parquet/")

    try:
        print("Carregando dados de municípios ...")

        municipios_path = os.path.join(nova_base_path, "municipios")
        cols_municipios = data_yaml["cnpj"]["cols_tb_municipios"]

        df_municipios = carregar_dados_parquet(municipios_path)
        df_municipios = df_municipios[cols_municipios]
        if debugging:
            print(df_municipios.head(10))

    except Exception as e:
        raise ("Erro ao carregar base de municípios", e)

    try:
        print("Carregando dados de estabelecimentos ...")
        estabelecimentos_path = os.path.join(nova_base_path, "estabelecimentos")
        colunas_estab = data_yaml["cnpj"]["cols_tb_estabelecimentos"]

        df_estabelecimentos = carregar_dados_parquet(estabelecimentos_path)
        df_estabelecimentos = df_estabelecimentos[colunas_estab]
        df_estabelecimentos.rename(
            columns={"municipio": "codigo_municipio"}, inplace=True
        )
        # df_estabelecimentos = aplica_formatacao(df_estabelecimentos)

        if debugging:
            print(df_estabelecimentos.head(10))

    except Exception as e:
        raise ("Erro ao carregar base de estabelecimentos", e)

    df_cno = pd.merge(
        df_estabelecimentos, df_municipios, on="codigo_municipio", how="inner"
    )

    dataframes = [df_estabelecimentos, df_municipios]
    for df in dataframes:
        del df

    if debugging:
        print("Número de linhas", df_cno.shape[0])
        print(df_cno.head(10))

    try:
        print("Carregando dados do simples nacional ...")
        simples_path = os.path.join(base_path, "simples")
        colunas_simples = data_yaml["cnpj"]["cols_tb_simples"]

        df_simples = carregar_dados_parquet(simples_path)
        df_simples = df_simples[colunas_simples]

        if debugging:
            print(df_simples.head(10))

    except Exception as e:
        raise ("Erro ao carregar base de empresas", e)

    df_cno = pd.merge(df_cno, df_simples, on="cnpj_basico", how="left")

    del df_simples

    if debugging:
        print(df_cno.head(10))

    output_path = os.path.join(
        base_path,
        "intermediario/estabelecimentos/intermediario.parquet",
    )

    print("Salva base Estabelecimentos ...")
    salva_parquet(df_cno, output_path)


def gera_df_cno_empresas(debugging=False):

    nova_base_path = os.path.join(base_path, "output_files/cnpj/processados_parquet/")

    try:
        print("Carregando dados de empresas ...")
        empresas_path = os.path.join(nova_base_path, "empresas")
        colunas_empresas = data_yaml["cnpj"]["cols_tb_empresas"]
        df_empresas = carregar_dados_parquet(empresas_path)
        df_empresas = df_empresas[colunas_empresas]
        df_empresas["cnpj_basico"] = df_empresas["cnpj_basico"].astype(str).str.zfill(8)
        df_empresas.rename(
            columns={"qualificacao_do_responsavel": "codigo_qualificacao"}, inplace=True
        )

        if debugging:
            print(df_empresas.head(10))

        output_path = os.path.join(
            base_path,
            "intermediario/empresas/empresas.parquet",
        )

        print("Salva base Empresas ...")
        salva_parquet(df_empresas, output_path)

    except Exception as e:
        raise ("Erro ao salvar base de Empresas", e)


def sei_la(debugging=False):

    nova_base_path = os.path.join(
        base_path, "output_files/cnpj/processados_parquet/intermediario/"
    )

    estabelecimentos_path = os.path.join(nova_base_path, "estabelecimentos")
    df_estabelecimentos = carregar_dados_parquet(estabelecimentos_path)
    if debugging:
        print(df_estabelecimentos.head())

    empresas_path = os.path.join(nova_base_path, "empresas")
    df_empresas = carregar_dados_parquet(empresas_path)
    if debugging:
        print(df_empresas.head())

    df_cno = pd.merge(df_estabelecimentos, df_empresas, on="cnpj_basico", how="left")

    dataframes = [df_estabelecimentos, df_empresas]
    for df in dataframes:
        del df

    df_cno["cnpj_ordem"] = df_cno["cnpj_ordem"].astype(str).str.zfill(4)
    df_cno["cnpj_dv"] = df_cno["cnpj_dv"].astype(str).str.zfill(2)

    try:
        print("Carregando dados de qualificação do responsável ...")
        qualificacao_path = os.path.join(
            base_path, "output_files/cnpj/processados_parquet/qualificacoes/"
        )

        colunas_qualificacoes = data_yaml["cnpj"]["cols_tb_qualificacoes"]
        df_qualificacoes = carregar_dados_parquet(qualificacao_path)
        df_qualificacoes = df_qualificacoes[colunas_qualificacoes]

        if debugging:
            print(df_qualificacoes.head(10))

    except Exception as e:
        raise ("Erro ao carregar base de qualificação do responsável", e)

    print("Vinculando dados de qualificações ..")
    df_cno = pd.merge(df_cno, df_qualificacoes, on="codigo_qualificacao", how="left")

    dataframes = [df_qualificacoes]
    for df in dataframes:
        del df

    print(df_cno.columns)
    print(df_cno.head())
