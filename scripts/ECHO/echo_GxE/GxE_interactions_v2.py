"""
GxE Knowledge Analysis in the ECHO Environment
==============================================

VERSION: 2

Overview:
This script is designed for GxE (Gene-Exposome interaction) analysis within
the ECHO environment. It builds upon previously generated interactions, which
are used as input parameters. The interactions are assumed to have been
created using the "Position_to_models.py" script.

Input Parameters:
1. Interaction Data: The interactions obtained from the "Position_to_models.py"
2. PLINK File: This file is read using the IGEM "read_plink" function, loading
only the file mapping into memory.
3. Exposome Archive: Data representing the exposome.
4. Covariants and Result File: Additional data to complement the analysis
model.

Key Features of Version 2:
- In this second version of the script, a unique data structure is created,
filtered to include only interactions found in GE.db. The analysis is
performed using multiprocessing, allowing for more memory allocation. This
reduces the I/O compared to version 1, making it suitable for environments
with real cluster usage, as HPC clusters typically have dedicated I/O
resources.
- The output is a file in the format of the "Interaction_study()" results.

Important Notes:
- The "Interaction_study()" function does not accept terms with ":" (e.g.,
Chromosome:Position) as input. Before running regressions, you'll need to
replace ":" with an underscore "_" and can revert it back to ":" after
execution.
- Genotype coding is not included in this script. Attempting to read PLINK
files with Pandas-genomics may not be feasible due to memory constraints.

Adjusting Genotype Coding:
- If you need to change the code to an additive model, manual adjustments can
be made within the script.

Compatibility:
- This script is based on IGEM version 0.1.6. Ensure that your ECHO HPC
environment is updated to support this version of IGEM.

Usage:
- Please ensure that you have prepared the necessary input data and have met
the prerequisites for executing this script within your ECHO environment.

Note: This script provides a more memory-efficient and parallelized approach
to GxE analysis compared to its previous version, making it suitable for
cluster-based environments with ample memory and computational resources.

We are using igem_lite (Same version to ECHO)
"""


import concurrent.futures

# import os
# import sys
import time

# from os.path import dirname, realpath
from os.path import join
from pathlib import Path

import pandas as pd
from igem_lite import epc
from igem_lite.load.plink import plink1_xa

# try:
#     v_root = Path(__file__).parents[3]
#     sys.path.append(os.path.abspath(v_root))
# except Exception as e:
#     print("Error: ", e)
#     raise


def run_interaction_study(args):
    g, e, df_data, ls_outcomes, ls_covariants = args
    interaction_study = epc.analyze.interaction_study(
        data=df_data,
        outcomes=ls_outcomes,
        interactions=[(g, e)],
        covariates=ls_covariants,
        min_n=100
    )
    # Reset the index to convert MultiIndex to columns
    interaction_study = interaction_study.reset_index()
    return interaction_study.to_dict(orient='split')


# START SCRIPT
v_time_process = time.time()

# Set parameters:
root_path = Path(__file__).parents[3]
data_path = root_path / "IGEM_Support_data" / "scripts" / "ECHO" / "echo_GxE" / "data_files" # noqa E501

# Setup the files
g_file = join(data_path, "genomics_xyz")
e_file = join(data_path, "exposomes_xyz.csv")
i_file = join(data_path, "result_4_model_xyz.txt")

# Setup the Outcomes and Covariantes
ls_outcomes = ["DIABETES"]
ls_covariants = ["RIDAGEYR", "female", "male", "black"]

# Read the Interactions Terms (G x E)
# TODO: Set the change ":" to "_"
df_interactions = pd.read_csv(i_file)
ls_exposomes = df_interactions["string"].tolist()
ls_positions = df_interactions["chromosome:position"].tolist()

# READ FILES
# 1 - Read the PLINK File
G = plink1_xa(g_file + ".bed", g_file + ".bim", g_file + ".fam", verbose=False)
# TODO: configure with ECHO data structure

# Filter ls_positions based on existing values in the 'variant' dimension
valid_positions_mask = G['variant']['snp'].isin(ls_positions)

# Select genotypes for the valid positions using boolean indexing
variant_data = G.sel(variant=valid_positions_mask)

# Convert to a Pandas DataFrame with each SNP as a separate column
df_genomic = variant_data.to_dataframe().unstack('variant').reset_index()
df_genomic.columns = [
    '_'.join(col).strip() for col in df_genomic.columns.values
    ]
df_genomic = df_genomic[
    ['sample_'] + [col for col in df_genomic.columns if 'genotype' in col]
    ]

# This is not necessary - used only to fix with NHAMES data
df_genomic = df_genomic.rename(
    columns={
        'genotype_variant0': 'chr1_864083',
        'genotype_variant1': 'chr1_887001',
        'genotype_variant2': 'chr1_1220751',
        'genotype_variant3': 'chr1_1208784',
        'genotype_variant4': 'chr1_86'
        })
df_genomic['sample_'] = df_genomic['sample_'].astype('int')

# 2 - Read the Exposome File
# Get only columns with interactions
E = pd.read_csv(e_file, usecols=['SEQN'] + ls_outcomes + ls_covariants + ls_exposomes)  # noqa E501
# TODO: Add all treatment to ECHO Data
E = E.rename(columns={'SEQN': 'ID'})
# E.memory_usage(deep=True).sum() / 1048576

# 3 - Merge the G and E in a single DataFrame to improve performance
df_data = E.merge(df_genomic, left_on='ID', right_on='sample_')
df_data = df_data.set_index('ID')

results = []

# Prepare arguments for the interaction study
interaction_args = [
    (
        x[1][0],
        x[1][1],
        df_data,
        ls_outcomes,
        ls_covariants
    ) for x in df_interactions.iterrows()
    ]

# Number of worker threads (adjust as needed)
num_workers = 12

# Run interaction studies in parallel using ThreadPoolExecutor
with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor: # noqa E501
    results = list(executor.map(run_interaction_study, interaction_args))

result_df_list = [
    pd.DataFrame(
        result['data'],
        columns=result['columns']
        ) for result in results]

# Concatenate the list of DataFrames into a single DataFrame
combined_df = pd.concat(result_df_list)

v_time = int(time.time() - v_time_process)
print(v_time)
# print(results)
