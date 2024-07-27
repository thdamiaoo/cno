import sys
import os
import yaml

sys.path.insert(0, os.path.abspath("app/dags/"))
# from cno.modules.tasks.utils import download_and_extract_zip
from dags.cno.modules.tasks.fn_pipeline import run_pipeline

if __name__ == "__main__":

    cno = True
    base_path = "/app/dags/cno/modules/data/"
    output_dir_template = "/app/dags/cno/modules/data/input_files/{}/"

    with open(base_path + "translate/translate.yaml", "r") as file:
        data_yaml = yaml.safe_load(file)

    if cno:
        output_dir = output_dir_template.format("cno")
        print(f"output_dir: {output_dir}")
        url = data_yaml["cnpj"]["url_cno"][0]
        print(f"url: {url}")
    else:
        arquivo = url = data_yaml["cnpj"]["tabela"]
        output_dir = output_dir_template.format("cnpj")
        print(f"output_dir: {output_dir}")
        # url = data_yaml["cnpj"]["url_cnpj"].format("")

    # download_and_extract_zip()
    run_pipeline(debugging=False)
