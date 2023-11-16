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

try:
    v_root = Path(__file__).parents[3]
    sys.path.append(os.path.abspath(v_root))
except Exception as e:
    print("Error: ", e)
    raise

from igem import epc
from igem.load.plink import plink1_xa


def load_genomic_data(bed_path, bim_path, fam_path):
    G = plink1_xa(bed_path, bim_path, fam_path, verbose=False)
    return G


def load_exposome_data(e_file, ls_outcomes, ls_covariants, ls_exposomes):
    E = dd.read_csv(e_file, assume_missing=True, usecols=['SEQN'] + ls_outcomes + ls_covariants + ls_exposomes)  # noqa E501
    E = E.rename(columns={'SEQN': 'ID'})
    return E


def run_interaction_study(G, E, g, e, ls_outcomes, ls_covariants):
    variant_data = G.where(G.snp == g, drop=True)
    df_genomic = variant_data.to_dataframe().reset_index()

    df_exposome = E[['ID'] + ls_covariants + ls_outcomes + [e]]
    df_genomic['sample'] = df_genomic['sample'].astype(float)
    df_data = df_exposome.merge(df_genomic, left_on='ID', right_on='sample')
    df_data = df_data.set_index('ID')
    df_data = df_data.rename(columns={'genotype': g})

    interaction_study = epc.analyze.interaction_study(
        data=df_data,
        outcomes=ls_outcomes,
        interactions=[(g, e)],
        covariates=ls_covariants,
        min_n=100
    )

    return {
        "Interaction": interaction_study.LRT_pvalue.index.levels[2][0],
        "Genomic_Variable": interaction_study.LRT_pvalue.index.levels[0][0],
        "Exposome_Variable": interaction_study.LRT_pvalue.index.levels[1][0],
        "Converged": interaction_study.Converged.values[0],
        "LRT_Pvalue": interaction_study.LRT_pvalue.values[0],
        "Adjusted_Pvalue": interaction_study.LRT_pvalue.values[0] * len(df_data)  # noqa E501
    }


def main(g_file, e_file, i_file, ls_outcomes, ls_covariants):
    df_interactions = pd.read_csv(i_file)
    data_tuples = [tuple(row) for row in df_interactions.itertuples(index=False)]  # noqa E501
    ls_exposomes = df_interactions["string"].tolist()

    interactions_bag = dask.bag.from_sequence(data_tuples)

    G = load_genomic_data(g_file + ".bed", g_file + ".bim", g_file + ".fam")
    E = load_exposome_data(e_file, ls_outcomes, ls_covariants, ls_exposomes)

    interactions_results = interactions_bag.map(
        lambda x: delayed(run_interaction_study)(G, E, x[0], x[1], ls_outcomes, ls_covariants)  # noqa E501
    )

    results = dask.compute(*interactions_results)
    return results


if __name__ == '__main__':
    multiprocessing.freeze_support()

    datafiles = join(dirname(realpath(__file__)), "data_files")
    g_file = join(datafiles, "genomics_xyz")
    e_file = join(datafiles, "exposomes_xyz.csv")
    i_file = join(datafiles, "result_4_model_xyz.txt")

    ls_outcomes = ["DIABETES"]
    ls_covariants = ["RIDAGEYR", "female", "male", "black"]

    result = main(g_file, e_file, i_file, ls_outcomes, ls_covariants)
    print(result)

    # TODO: definir uma lista de columnas que serao lidas do Exposome

    # These process with 30 models get 40 minutes to run and used 200Gb. 