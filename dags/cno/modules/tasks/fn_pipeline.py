# cno/modules/tasks/fn_pipeline.py

import os
import sys
import pandas as pd
import yaml
from datetime import datetime

sys.path.insert(0, os.path.abspath("/home/thdamiao/projects/cno/dags/"))

from cno.modules.tasks.utils import (
    csv_to_pandas,
    rename_columns,
    create_dataframe,
    save_to_csv,
)


def run_pipeline(debugging=False):

    file_names = ["cno", "cno_areas", "cno_cnaes", "cno_totais", "cno_vinculos"]
    base_path = "/home/thdamiao/projects/cno/dags/cno/modules/data/"

    with open(base_path + "translate/translate.yaml", "r") as file:
        data_yaml = yaml.safe_load(file)

    # CARREGA YAML
    data_dict_cno_qualif_resp = data_yaml["cno"]["cno_qualicicacao_resposavel"][
        "data_dict"
    ]

    data_dict_cno_situacao = data_yaml["cno"]["cno_situacao"]["data_dict"]

    data_dict_cno_vinculos_contrib = data_yaml["cno_vinculos"][
        "cno_vinculos_qualicicacao_contribuinte"
    ]["data_dict"]

    # CRIA DF A PARTIR DO YAML
    df_qualif_resp = create_dataframe(data_dict_cno_qualif_resp)
    df_cno_situacao = create_dataframe(data_dict_cno_situacao)
    df_cno_vin_contrib = create_dataframe(data_dict_cno_vinculos_contrib)

    dfs = {}

    for file_name in file_names:
        path = os.path.join(base_path + "input_files", f"{file_name}.csv")
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
                on=["cno", "data_de_inicio", "data_de_registro", "ni_do_responsavel"],
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

        sufixo = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        save_to_csv(df_cno, base_path + f"output_files/tbl_fato_cno_{sufixo}.csv")

    except Exception as e:
        raise ("Erro ao gerar base de CNO", e)
