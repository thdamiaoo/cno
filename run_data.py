import sys
import os

sys.path.insert(0, os.path.abspath("app/dags/"))
from dags.cno.modules.tasks.fn_pipeline import run_pipeline

if __name__ == "__main__":

    run_pipeline(debugging=False)
