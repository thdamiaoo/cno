import sys
import os
import yaml

sys.path.insert(0, os.path.abspath("app/dags/"))
from dags.cno.modules.tasks.utils import import_csv_to_db

if __name__ == "__main__":

    csv_path = "/app/dags/cno/modules/data/output_files/"

    csv_files = {
        csv_path + "tbl_fato_cno_centro_oeste.csv": "tb_fato_cno",
        csv_path + "tbl_fato_cno_nordeste.csv": "tb_fato_cno",
        csv_path + "tbl_fato_cno_norte.csv": "tb_fato_cno",
        csv_path + "tbl_fato_cno_sudeste.csv": "tb_fato_cno",
        csv_path + "tbl_fato_cno_sul.csv": "tb_fato_cno",
    }

    for csv_path, table_name in csv_files.items():
        import_csv_to_db(csv_path, "tb_fato_cno")
