import concurrent.futures
import os
import sys
from pathlib import Path

import pandas as pd

try:
    v_root = Path(__file__).parents[2]
    sys.path.append(os.path.abspath(v_root))
except Exception as e:
    print("erro: ", e)
    raise

from igem import epc

# ------------------------------------------------------------
# STEP 1: Define Main Table, Outcomes and Covariantes
# ------------------------------------------------------------

# DataFrame to collect results
df_results_discover_final = pd.DataFrame()
df_results_replicate_final = pd.DataFrame()
# list_results_discover = []
# list_results_replicate = []

# Read NHANES Main Table
current_path = Path.cwd()
df_maintable = epc.load.from_csv(str(current_path / "scripts/ExE_InterationAnalysis/files/MainTable_sample.csv")) # noqa E501




# Using a ThreadPoolExecutor
with concurrent.futures.ThreadPoolExecutor() as executor:
    results = epc.analyze.my_library_function(df_maintable, executor=executor)

# Using a ProcessPoolExecutor
# with concurrent.futures.ProcessPoolExecutor() as executor:
#     results = epc.analyze.my_library_function(df_maintable, executor=executor)
print(results)





"""
# list of Outcomes
list_outcome = [
    "LBXHGB",
]

# list of Covariants
list_covariant = [
    "female",
    "black",
    "mexican",
    "other_hispanic",
    "other_eth",
    "SDDSRVYR",
    "SES_LEVEL",
]

# result = epc.analyze.exe_teste()
result = epc.analyze.exe_pairwise(
    df_maintable,
    list_outcome,
    list_covariant,
    report_betas=False,
    process_num=4
    )

print(result)
"""