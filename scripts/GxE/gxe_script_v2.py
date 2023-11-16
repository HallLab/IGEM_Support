# import numpy as np
# import pandas as pd
# import statsmodels.api as sm
# import xarray as xr
# from numpy import dtype, nan
# from numpy.testing import assert_array_equal, assert_equal

# import multiprocessing
import os
import sys
from os.path import dirname, join, realpath
from pathlib import Path

import dask.dataframe as dd

try:
    v_root = Path(__file__).parents[2]
    sys.path.append(os.path.abspath(v_root))
except Exception as e:
    print("erro: ", e)
    raise

from igem.load.plink import plink1_xa

# Load PLINK data and convert to DataArray (G)
datafiles = join(dirname(realpath(__file__)), "data_files")
file_prefix = join(datafiles, "LURIC_AFFY_FINAL_clean")
bim = file_prefix + ".bim"
bed = file_prefix + ".bed"
fam = file_prefix + ".fam"

G = plink1_xa(bed, bim, fam, verbose=False)

# To create a list of all values in the 'variant' dimension:
ls_genomic = G['variant'].values.tolist()
# variant_values = G['snp'].values.tolist()

## Load exposome data as Dask DataFrame (E)
# Read the CSV file using dask
exposome_file = join(datafiles, "exposomes.csv")
E = dd.read_csv(exposome_file)

# Add all columns to a list
E_columns_list = E.columns.tolist()

# Perform operations to select Columns
# 1. Change Category: E['female'] = E['female'].astype('category')
# 2. Create list to Outcomes, Covariants and Regression Variables (if apply)

ls_covariants = ["RIDAGEYR", "female", "male", "black"]  # Add and Change it!
ls_outcome = ["DIABETES"]  # This is just a Example!
ls_exposomes = [variable for variable in E_columns_list if variable not in ls_covariants and variable not in ls_outcome]


# Define SEQN as ID
E = E.rename(columns={'SEQN': 'ID'})

# Define ID as Index
# E = E.set_index('first_column_name')

# for each Variant/SNP (G) to correlate with a Variable (E)
for g in ls_genomic:
    # G - Access only the variant interacted
    variant_data = G.sel(variant=g)
    df_genomic = variant_data.to_dataframe().reset_index()

    # for each Exposome (E)
    for e in ls_exposomes:
        # Here I need to get the columns ID, in ls_covariants, ls_outcome and e and create a df_exposome

        # link this dataframe with the df_genomic by df_genomic.sample = df_exposome.ID / the merge in df_data

        # run the function:
         # Run Interation Study
        # Interation_Study = epc.analyze.interaction_study(
        #     data=df_data,
        #     outcomes=ls_outcome,
        #     interactions=[(g, e)],
        #     covariates=ls_covariant,
        # )
        #  # Save results in list: outcome/e1/e2/converged/LRT_pvalue/Bonfp
        # Interation_Study_List.append(
        #     [
        #         Interation_Study.LRT_pvalue.index.levels[2][0],
        #         Interation_Study.LRT_pvalue.index.levels[0][0],
        #         Interation_Study.LRT_pvalue.index.levels[1][0],
        #         Interation_Study.Converged.values[0],
        #         Interation_Study.LRT_pvalue.values[0],
        #         Interation_Study.LRT_pvalue.values[0] * len(df_nhanes_map),
        #     ]
        # )




import dask.dataframe as dd
import dask.delayed
import dask.distributed
import dask_ml.linear_model as dm_lm

# Establish a Dask client for parallel processing
client = dask.distributed.Client()

# Read specific columns from G and E as Dask DataFrames
G_dask = dd.from_pandas(G[['IID', 'VariantColumn']], npartitions=n_partitions)
E_dask = dd.from_pandas(E[['IID', 'ExposomeColumn1', 'ExposomeColumn2', ...]], npartitions=n_partitions)

# Merge G_dask and E_dask on IID
merged_dask = G_dask.merge(E_dask, on='IID')

# Define a function for interaction analysis
def analyze_interaction(variant_col, exposome_col):
    # Create the interaction term (example: variant * exposome)
    interaction_col = variant_col * exposome_col

    # Fit a linear model
    model = dm_lm.LinearRegression()
    model.fit(interaction_col.values.reshape(-1, 1), target.values)

    return model

# Create a list of delayed objects for interaction analysis
interactions = []
for variant_col in G_dask.columns:
    for exposome_col in E_dask.columns:
        interaction = dask.delayed(analyze_interaction)(merged_dask[variant_col], merged_dask[exposome_col])
        interactions.append(interaction)

# Compute the interactions in parallel
results = dask.compute(*interactions)

# Process the results to calculate FDR for each interaction

# Close the Dask client
client.close()



# # Merge G and E based on IID column
# merged_data = pd.merge(G.to_dataframe(), E, on='IID')

# # Function to run regression analysis and filtering
# def process_column(column_name, genotype_col, exposome_data):
#     merged_col_data = pd.merge(
#         pd.DataFrame({'IID': G['IID'].values, column_name: genotype_col}),
#         exposome_data, on='IID'
#         )

#     # Run regression analysis
#     X = sm.add_constant(merged_col_data['ExposomeColumn'])  # Modify with actual column name
#     model = sm.OLS(merged_col_data['GenotypeColumn'], X)
#     results = model.fit()

#     # Filter records found in both G and E
#     filtered_data = merged_col_data.dropna()

#     return column_name, results.summary(), filtered_data

# # Number of parallel processes
# num_processes = multiprocessing.cpu_count()  # Use all available cores

# # Initialize a multiprocessing Pool
# pool = multiprocessing.Pool(processes=num_processes)

# # List of tasks to be parallelized
# tasks = [(column, G[column].values, E) for column in G.dims if column != 'IID']

# # Map tasks to the pool of processes
# results = pool.starmap(process_column, tasks)

# # Close and join the pool
# pool.close()
# pool.join()

# # Convert results to dictionary
# regression_results = {col: summary for col, summary, _ in results}
# filtered_data = pd.concat([data for _, _, data in results])

# # Convert the dictionary of regression results to a DataFrame if needed
# regression_results_df = pd.concat([res.summary().tables[1] for col, res in regression_results.items()])

