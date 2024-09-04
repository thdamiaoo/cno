import sys
import os
import time

# sys.path.insert(0, os.path.abspath("app/dags/"))
sys.path.insert(0, os.path.abspath("dags/"))
from dags.cno.modules.tasks.fn_pipeline import (
    gera_base_intermediaria_cnpj,
    gera_df_cno_estabelecimentos,
    gera_df_cno_empresas,
    sei_la,
)

if __name__ == "__main__":
    # gera_base_intermediaria_cnpj(debugging=True)
    # gera_df_cno_estabelecimentos(debugging=False)
    # gera_df_cno_empresas(debugging=True)
    sei_la(debugging=True)
