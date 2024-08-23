import sys
import os
import yaml

# sys.path.insert(0, os.path.abspath("app/dags/"))
sys.path.insert(0, os.path.abspath("dags/"))
from dags.cno.modules.tasks.utils import download_and_extract_zip

if __name__ == "__main__":

    data = input("Insira data dos arquivos (YYYY-MM):")
    # base_path = "/cno/modules/data/"
    # output_dir_template = "/app/dags/cno/modules/data/input_files/{}/"
    
    base_path = "/home/thdamiao/projects/cno/dags/cno/modules/data/"
    output_dir_template = (
        "/home/thdamiao/projects/cno/dags/cno/modules/data/input_files/{}/"
    )

    with open(base_path + "translate/translate.yaml", "r") as file:
        data_yaml = yaml.safe_load(file)

    output_dir = output_dir_template.format("cnpj")
    print(f"output_dir: {output_dir}")

    tabelas = data_yaml["cnpj"]["nome_arquivos"]
    url_template = data_yaml["cnpj"]["url"][0]

    for tabela in tabelas:
        if '{numero}' in tabela:
            for numero in range(10):
                tabela_com_numero = tabela.format(numero=numero)
                url = url_template.format(data=data, tabela=tabela_com_numero)
                print(f"url: {url}")
                download_and_extract_zip(output_dir, url)
        else:
            url = url_template.format(data=data, tabela=tabela)
            print(f"url: {url}")
            download_and_extract_zip(output_dir, url)
