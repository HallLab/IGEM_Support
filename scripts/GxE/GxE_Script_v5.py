import multiprocessing
import os
import sys
from os.path import dirname, join, realpath
from pathlib import Path
from typing import Dict

import dask
import dask.dataframe as dd
from dask import delayed

# Add project root to sys.path
try:
    v_root = Path(__file__).parents[2]
    sys.path.append(os.path.abspath(v_root))
except Exception as e:
    print("Error: ", e)
    raise

# Import necessary modules from IGEM project
from igem import epc
from igem.load.plink import plink1_xa


def main() -> Dict:
    # Load PLINK data and convert to DataArray (G)
    datafiles = join(dirname(realpath(__file__)), "data_files")
    file_prefix = join(datafiles, "LURIC_AFFY_FINAL_clean")
    bim = file_prefix + ".bim"
    bed = file_prefix + ".bed"
    fam = file_prefix + ".fam"
    G = plink1_xa(bed, bim, fam, verbose=False)

    # Create lists of genomic and exposome variables
    ls_genomic = G['variant'].values.tolist()
    exposome_file = join(datafiles, "exposomes.csv")
    E = dd.read_csv(exposome_file, assume_missing=True)
    E_columns_list = E.columns.tolist()
    ls_covariants = ["RIDAGEYR", "female", "male", "black"]
    ls_outcome = ["DIABETES"]
    ls_exposomes = [variable for variable in E_columns_list if variable not in ls_covariants and variable not in ls_outcome]  # noqa E501

    # Create a Dask Bag for delayed computations
    interactions_bag = dask.bag.from_sequence(
        [(g, e) for g in ls_genomic for e in ls_exposomes]
        )

    def run_interaction_study(G, E, g, e, ls_outcome, ls_covariants):
        variant_data = G.sel(variant=g)
        df_genomic = variant_data.to_dataframe().reset_index()
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
        )

        return {
            "Interaction": Interation_Study.LRT_pvalue.index.levels[2][0],
            "Genomic_Variable": Interation_Study.LRT_pvalue.index.levels[0][0],
            "Exposome_Variable": Interation_Study.LRT_pvalue.index.levels[1][0],  # noqa E501
            "Converged": Interation_Study.Converged.values[0],
            "LRT_Pvalue": Interation_Study.LRT_pvalue.values[0],
            "Adjusted_Pvalue": Interation_Study.LRT_pvalue.values[0] * len(df_data),  # noqa E501
        }

    num_workers = multiprocessing.cpu_count()  # Adjust as needed
    interactions_results = interactions_bag.map(
        lambda x: delayed(
            run_interaction_study
            )(G, E, x[0], x[1], ls_outcome, ls_covariants),
        num_workers=num_workers
    )

    results = dask.compute(*interactions_results)
    return results


if __name__ == '__main__':
    multiprocessing.freeze_support()
    result = main()
    print(result)
