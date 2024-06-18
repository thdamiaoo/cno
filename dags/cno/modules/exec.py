import sys
import os

sys.path.insert(0, os.path.abspath("/home/thdamiao/projects/cno/dags/"))

# from cno.modules.tasks.utils import download_and_extract_zip
from cno.modules.tasks.fn_pipeline import run_pipeline

if __name__ == "__main__":
    # download_and_extract_zip()
    run_pipeline()