import concurrent.futures
import os
import sys
import time
from os.path import dirname, join, realpath
from pathlib import Path

import dask.dataframe as dd
import pandas as pd

try:
    v_root = Path(__file__).parents[3]
    sys.path.append(os.path.abspath(v_root))
except Exception as e:
    print("Error: ", e)
    raise

from igem import epc
from igem.load.plink import plink1_xa


def run_interaction_study(args):
    g, e, df_data, ls_outcomes, ls_covariants = args
    interaction_study = epc.analyze.interaction_study(
        data=df_data,
        outcomes=ls_outcomes,
        interactions=[(g, e)],
        covariates=ls_covariants,
        min_n=100
    )
    return interaction_study


v_time_process = time.time()


# Setup the files
datafiles = join(dirname(realpath(__file__)), "data_files")
g_file = join(datafiles, "genomics_xyz")
e_file = join(datafiles, "exposomes_xyz.csv")
i_file = join(datafiles, "result_4_model_xyz.txt")

# Setup the Outcomes and Covariantes
ls_outcomes = ["DIABETES"]
ls_covariants = ["RIDAGEYR", "female", "male", "black"]

# Read the Interactions Terms (G x E)
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
df_genomic.columns = ['_'.join(col).strip() for col in df_genomic.columns.values]
df_genomic = df_genomic[['sample_'] + [col for col in df_genomic.columns if 'genotype' in col]]

# This is not necessary - used only to fix with NHAMES data
df_genomic = df_genomic.rename(columns={'genotype_variant0': 'chr1_864083', 'genotype_variant1': 'chr1_887001', 'genotype_variant2': 'chr1_1220751', 'genotype_variant3': 'chr1_1208784', 'genotype_variant4': 'chr1_86'})
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

## ==> In Serial
# for x in df_interactions.iterrows():

#     g = x[1][0]
#     e = x[1][1]
#     print(g, " - ", e)

#     interaction_study = epc.analyze.interaction_study(
#         data=df_data,
#         outcomes=ls_outcomes,
#         interactions=[(g, e)],
#         covariates=ls_covariants,
#         min_n=100
#     )

#     results.append(interaction_study)

## ==> In Paralel
# Prepare arguments for the interaction study
interaction_args = [(x[1][0], x[1][1], df_data, ls_outcomes, ls_covariants) for x in df_interactions.iterrows()]

# Number of worker threads (adjust as needed)
num_workers = 12

# Run interaction studies in parallel using ThreadPoolExecutor
with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
    results = list(executor.map(run_interaction_study, interaction_args))


v_time = int(time.time() - v_time_process)
print(v_time)
# print(results)
