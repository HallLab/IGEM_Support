"""
This script is designed to facilitate the generation of models for GxE (
    Gene-Exposome interaction) analysis, leveraging the GE.db knowledge base.

Input:
- Two input files are required: one containing Chromosome and Position data,
and the other containing Exposome terms to be filtered.
- The script utilizes the IGEM Filter function for its processing.

Parameters:
- The script accepts several parameters:
    - Assembly Version: Choose between assembly versions 37 or 38.
    - Pair Range: Specify the range of pairs to expand the scope of gene
    search.
    - Prohibited Format: Define the format for prohibited items.

Output:
The script produces four output files:
1. Enriched Chromosome and Position Data: This file retains the original
structure of the input Chromosome and Position data but includes localized
genes.
2. Gene-Term Associations: This file contains the results of the gene search
in the IGEM database, returning associated terms for the located genes.
3. Filtered Interactions: This file displays interactions found in the TermMap
table, filtering only the genes from the previous file (Gene-Term Associations)
and the Exposomes specified in the input file.
4. Interaction Models: This file presents pairs of interactions located in the
knowledge base, formatted to match the structure used in the target system or
database for further analysis.
"""

# TODO: RUN IN DEBUG MODE

import os
from pathlib import Path

import igem

files_sch = [
    '10301_sch_maf05_pruned3.txt',
    '10301_sch_maf05_pruned5.txt',
    '10701_sch_maf05_pruned3.txt',
    '10701_sch_maf05_pruned5.txt',
    '11601_sch_maf05_pruned3.txt',
    '11601_sch_maf05_pruned5.txt',
    '11901_affy5_sch_maf05_pruned3.txt',
    '11901_affy5_sch_maf05_pruned5.txt',
    '11901_affy6_sch_maf05_pruned3.txt',
    '11901_affy6_sch_maf05_pruned5.txt',
    '12801_sch_maf05_pruned3.txt',
    '12801_sch_maf05_pruned5.txt',
]

files_pre = [
    '10701_pre_maf05_pruned3.txt',
    '10701_pre_maf05_pruned5.txt',
    '11901_affy5_pre_maf05_pruned3.txt',
    '11901_affy5_pre_maf05_pruned5.txt',
    '11901_affy6_pre_maf05_pruned3.txt',
    '11901_affy6_pre_maf05_pruned5.txt',
    '12801_pre_maf05_pruned3.txt',
    '12801_pre_maf05_pruned5.txt',
]

for file in files_sch:

    # Define the names of your input files
    file_name_positions = file
    file_name_exposomes = "sch_echo_words_clean.csv"

    # Get the directory where the script is located
    root_path = Path(__file__).parents[3]
    data_path = root_path / "IGEM_Support_data" / "scripts" / "ECHO" / "echo_genes_prod" / "output_files" # noqa E501

    # Construct the full paths to your input files
    input_positions = os.path.join(data_path, file_name_positions)
    input_exposomes = os.path.join(data_path, file_name_exposomes)

    chk_result = igem.ge.filter.positions_to_term(
            input_positions,
            input_exposomes,
            assembly=37,
            boundaries=10000,
            delimiter="\\t",
            )

    print(chk_result, 'file process: ', file)


for file in files_pre:

    # Define the names of your input files
    file_name_positions = file
    file_name_exposomes = "pre_echo_words_clean.csv"

    # Get the directory where the script is located
    root_path = Path(__file__).parents[3]
    data_path = root_path / "IGEM_Support_data" / "scripts" / "ECHO" / "echo_genes_prod" / "output_files" # noqa E501

    # Construct the full paths to your input files
    input_positions = os.path.join(data_path, file_name_positions)
    input_exposomes = os.path.join(data_path, file_name_exposomes)

    chk_result = igem.ge.filter.positions_to_term(
            input_positions,
            input_exposomes,
            assembly=37,
            boundaries=10000,
            delimiter="\\t",
            )

    print(chk_result, 'file process: ', file)