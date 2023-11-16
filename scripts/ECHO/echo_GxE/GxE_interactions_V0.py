"""
Strip to run GxE Interactions on ECHO HPC
"""

import multiprocessing
import os
import sys
from os.path import dirname, join, realpath
from pathlib import Path
from typing import Dict

import dask
import dask.dataframe as dd
import pandas as pd
from dask import delayed

# Add project root to sys.path
try:
    v_root = Path(__file__).parents[3]
    sys.path.append(os.path.abspath(v_root))
except Exception as e:
    print("Error: ", e)
    raise

# Import necessary modules from IGEM project
from igem import epc
from igem.load.plink import plink1_xa


def main(
        g_file="path",
        e_file="path",
        i_file="path",
        ls_outcomes=[],
        ls_covariants=[],
        ) -> Dict:

    # STEP 1: GE.filter INTERACTIONS
    # ------------------------------
    # Read the GE.filter interactions list
    df_interactions = pd.read_csv(i_file)
    # Create a list of tuples from the DataFrame
    data_tuples = [
        tuple(row) for row in df_interactions.itertuples(index=False)
        ]
    # Create a Dask Bag for delayed computations
    interactions_bag = dask.bag.from_sequence(data_tuples)

    # STEP 2: GENOMICS DATA
    # ---------------------
    # Load PLINK data and convert to DataArray (G)
    bim = g_file + ".bim"
    bed = g_file + ".bed"
    fam = g_file + ".fam"
    G = plink1_xa(bed, bim, fam, verbose=False)

    # Create lists of genomic and exposome variables
    # TODO: Alter the "SNP" to the correct attr
    # ls_genomic = G['variant']["snp"].values.tolist()

    # TODO: Filter to keep only Variantes in the df_interactions

    # STEP 3: EXPOSOME DATA
    # ---------------------
    # Load Exposome data in dask datadrame
    E = dd.read_csv(e_file, assume_missing=True)
    # TODO: Setup ID
    E = E.rename(columns={'SEQN': "ID"})

    # E_columns_list = E.columns.tolist()

    # TODO: Check if Covariants is in the same file
    # ls_exposomes = [variable for variable in E_columns_list if variable not in ls_covariants and variable not in ls_outcomes]  # noqa E501

    # STEP x: Functions to filter data and run analysis
    def run_interaction_study(G, E, g, e, ls_outcome, ls_covariants):
        # get g position data from G
        # TODO: replace snp to the correct PLINK ATTR
        variant_data = G.where(G.snp == g, drop=True)
        # Convert to DataFrame with all att in the position
        df_genomic = variant_data.to_dataframe().reset_index()

        # TODO: Check what is the ID Column on Exposome file
        df_exposome = E[['ID'] + ls_covariants + ls_outcome + [e]]
        df_genomic['sample'] = df_genomic['sample'].astype(float)
        df_data = df_exposome.merge(
            df_genomic, left_on='ID', right_on='sample'
            )
        df_data = df_data.set_index('ID')
        df_data = df_data.rename(columns={"genotype": g})

        Interation_Study = epc.analyze.interaction_study(
            data=df_data,
            outcomes=ls_outcome,
            interactions=[(g, e)],
            covariates=ls_covariants,
            min_n=100,
        )

        return {
            "Interaction": Interation_Study.LRT_pvalue.index.levels[2][0],
            "Genomic_Variable": Interation_Study.LRT_pvalue.index.levels[0][0],
            "Exposome_Variable": Interation_Study.LRT_pvalue.index.levels[1][0],  # noqa E501
            "Converged": Interation_Study.Converged.values[0],
            "LRT_Pvalue": Interation_Study.LRT_pvalue.values[0],
            "Adjusted_Pvalue": Interation_Study.LRT_pvalue.values[0] * len(df_data),  # noqa E501
        }

    # STEP 4: create the map of interactions to process
    num_workers = multiprocessing.cpu_count()  # Adjust as needed
    num_workers = 1
    interactions_results = interactions_bag.map(
        lambda x: delayed(
            run_interaction_study
            )(G, E, x[0], x[1], ls_outcomes, ls_covariants),
        num_workers=num_workers
    )

    # #STEP 5: Process map and save in results
    results = dask.compute(*interactions_results)
    return results


if __name__ == '__main__':
    multiprocessing.freeze_support()

    # Setup files
    datafiles = join(dirname(realpath(__file__)), "data_files")
    g_file = join(datafiles, "genomics_xyz")  # 10 files (pre and sch: 10301, 10701 ... 12801)
    e_file = join(datafiles, "exposomes_xyz.csv")  # 2 files (pre and sch)
    i_file = join(datafiles, "result_4_model_xyz.txt") # 10 files

    # Setup the Outcome and Covariants
    ls_outcomes = ["DIABETES"]
    ls_covariants = ["RIDAGEYR", "female", "male", "black"]

    result = main(g_file, e_file, i_file, ls_outcomes, ls_covariants)
    print(result)
