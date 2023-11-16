import multiprocessing
from os.path import dirname, join, realpath

import numpy as np
import pandas as pd
import statsmodels.api as sm
import xarray as xr
from numpy import dtype, nan
from numpy.testing import assert_array_equal, assert_equal

from igem.load.plink import plink1_xa

# Load PLINK data and convert to DataArray (G)
datafiles = join(dirname(realpath(__file__)), "data_files")
file_prefix = join(datafiles, "LURIC_AFFY_FINAL_clean")
bim = file_prefix + ".bim"
bed = file_prefix + ".bed"
fam = file_prefix + ".fam"

G = plink1_xa(bed, bim, fam, verbose=False)

# Load exposome data as DataFrame (E)



# Merge G and E based on IID column
merged_data = pd.merge(G.to_dataframe(), E, on='IID')

# Function to run regression analysis and filtering
def process_column(column_name, genotype_col, exposome_data):
    merged_col_data = pd.merge(
        pd.DataFrame({'IID': G['IID'].values, column_name: genotype_col}),
        exposome_data, on='IID'
        )

    # Run regression analysis
    X = sm.add_constant(merged_col_data['ExposomeColumn'])  # Modify with actual column name
    model = sm.OLS(merged_col_data['GenotypeColumn'], X)
    results = model.fit()

    # Filter records found in both G and E
    filtered_data = merged_col_data.dropna()

    return column_name, results.summary(), filtered_data

# Number of parallel processes
num_processes = multiprocessing.cpu_count()  # Use all available cores

# Initialize a multiprocessing Pool
pool = multiprocessing.Pool(processes=num_processes)

# List of tasks to be parallelized
tasks = [(column, G[column].values, E) for column in G.dims if column != 'IID']

# Map tasks to the pool of processes
results = pool.starmap(process_column, tasks)

# Close and join the pool
pool.close()
pool.join()

# Convert results to dictionary
regression_results = {col: summary for col, summary, _ in results}
filtered_data = pd.concat([data for _, _, data in results])

# Convert the dictionary of regression results to a DataFrame if needed
regression_results_df = pd.concat([res.summary().tables[1] for col, res in regression_results.items()])






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
