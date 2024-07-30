import sys
import os
import yaml

# sys.path.insert(0, os.path.abspath("app/dags/"))
sys.path.insert(0, os.path.abspath("dags/"))
from dags.cno.modules.tasks.utils import download_and_extract_zip

if __name__ == "__main__":

    # base_path = "/app/dags/cno/modules/data/"
    base_path = "/home/thdamiao/projects/cno/dags/cno/modules/data/"
    # output_dir_template = "/app/dags/cno/modules/data/input_files/{}/"
    output_dir_template = (
        "/home/thdamiao/projects/cno/dags/cno/modules/data/input_files/{}/"
    )

    with open(base_path + "translate/translate.yaml", "r") as file:
        data_yaml = yaml.safe_load(file)

    output_dir = output_dir_template.format("cno")
    print(f"output_dir: {output_dir}")
    url = data_yaml["cno"]["url"][0]
    print(f"url: {url}")

    download_and_extract_zip(output_dir, url)
