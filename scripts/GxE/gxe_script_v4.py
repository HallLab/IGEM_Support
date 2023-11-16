import multiprocessing
import os
import sys
from os.path import dirname, join, realpath
from pathlib import Path

import dask
import dask.dataframe as dd
from dask import delayed

try:
    v_root = Path(__file__).parents[2]
    sys.path.append(os.path.abspath(v_root))
except Exception as e:
    print("error: ", e)
    raise

from igem import epc
from igem.load.plink import plink1_xa


def main():
    # try:
    #     v_root = Path(__file__).parents[2]
    #     sys.path.append(os.path.abspath(v_root))
    # except Exception as e:
    #     print("error: ", e)
    #     raise
    # Library to load DJANGO

    # from igem import epc
    # from igem.load.plink import plink1_xa

    # Load PLINK data and convert to DataArray (G)
    datafiles = join(dirname(realpath(__file__)), "data_files")
    file_prefix = join(datafiles, "LURIC_AFFY_FINAL_clean")
    bim = file_prefix + ".bim"
    bed = file_prefix + ".bed"
    fam = file_prefix + ".fam"

    G = plink1_xa(bed, bim, fam, verbose=False)

    # To create a list of all values in the 'variant' dimension:
    ls_genomic = G['variant'].values.tolist()

    ## Load exposome data as Dask DataFrame (E)
    # Read the CSV file using dask
    exposome_file = join(datafiles, "exposomes.csv")
    E = dd.read_csv(exposome_file, assume_missing=True)

    # Add all columns to a list (E_columns_list)
    E_columns_list = E.columns.tolist()

    # # Specify data types for all columns as float64
    # column_dtypes = {column: 'float64' for column in E_columns_list}

    # # Set the data types for the columns
    # E = E.astype(column_dtypes)

    # Perform operations to select Columns
    # 1. Change Category: E['female'] = E['female'].astype('category')
    # 2. Create list to Outcomes, Covariants and Regression Variables (if apply)

    # Define SEQN as ID
    E = E.rename(columns={'SEQN': 'ID'})
    # Define ID as Index
    # E = E.set_index('first_column_name')

    ls_covariants = ["RIDAGEYR", "female", "male", "black"]  # Add and Change it!
    ls_outcome = ["DIABETES"]  # This is just an example!
    ls_exposomes = [variable for variable in E_columns_list if variable not in ls_covariants and variable not in ls_outcome]

    # DUEBUG:
    ls_exposomes = ["LBX167", "LBXD01"]
    ls_genomic = ["variant0", "variant1"]

    # Create a Dask Bag to store the delayed computations for each interaction
    interactions_bag = dask.bag.from_sequence([(g, e) for g in ls_genomic for e in ls_exposomes])

    # Define a function that runs the interaction study for a given GxE pair
    def run_interaction_study(G, E, g, e, ls_outcome, ls_covariants):
        # Access only the variant interacted
        variant_data = G.sel(variant=g)
        df_genomic = variant_data.to_dataframe().reset_index()

        # Create a DataFrame for exposome data
        df_exposome = E[['ID'] + ls_covariants + ls_outcome + [e]]

        # Merge df_genomic with df_exposome
        df_genomic['sample'] = df_genomic['sample'].astype(float)
        df_data = df_exposome.merge(df_genomic, left_on='ID', right_on='sample')
        # Define ID as Index
        df_data = df_data.set_index('ID')
        df_data = df_data.rename(columns={"genotype": g})

        # Run the interaction study
        Interation_Study = epc.analyze.interaction_study(
            data=df_data,
            outcomes=ls_outcome,
            interactions=[(g, e)],
            covariates=ls_covariants,
        )

        # Return the results as a tuple
        return (
            Interation_Study.LRT_pvalue.index.levels[2][0],
            Interation_Study.LRT_pvalue.index.levels[0][0],
            Interation_Study.LRT_pvalue.index.levels[1][0],
            Interation_Study.Converged.values[0],
            Interation_Study.LRT_pvalue.values[0],
            Interation_Study.LRT_pvalue.values[0] * len(df_data),
        )

    num_workers = 6
    # Use dask.bag.map to apply the function to all interactions in parallel
    num_workers = multiprocessing.cpu_count()  # Adjust as needed
    interactions_results = interactions_bag.map(
        lambda x: delayed(run_interaction_study)(G, E, x[0], x[1], ls_outcome, ls_covariants),
        # num_workers=num_workers  # Corrected argument name
    )

    # Compute the results using dask.compute()
    results = dask.compute(*interactions_results)

    return results
    # Now, 'results' contains the results of the interaction studies for all GxE pairs

    # Further processing or analysis of the results can be done here

    # Close the Dask client if needed
    # dask.distributed.Client().close()

if __name__ == '__main__':
    # Ensure that multiprocessing code is executed correctly
    multiprocessing.freeze_support()

    # Call the main function
    dados = main()
    print(dados)

