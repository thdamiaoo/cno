import sys
import os

# sys.path.insert(0, os.path.abspath("app/dags/"))
sys.path.insert(0, os.path.abspath("dags/"))
from dags.cno.modules.tasks.fn_pipeline import (
    gera_base_intermediaria_cnpj,
    run_pipeline_cnpj,
)

if __name__ == "__main__":
    # gera_base_intermediaria_cnpj(debugging=True)
    run_pipeline_cnpj(debugging=True)
