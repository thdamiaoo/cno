import sys
import os

# sys.path.insert(0, os.path.abspath("app/dags/"))
sys.path.insert(0, os.path.abspath("dags/"))
from dags.cno.modules.tasks.fn_pipeline import run_pipeline_cnpj

if __name__ == "__main__":

    run_pipeline_cnpj(debugging=True)
