import os
import sys
from pathlib import Path

import pandas as pd
import pyliftover
from pyensembl import EnsemblRelease

try:
    v_root = Path(__file__).parents[3]
    sys.path.append(os.path.abspath(v_root))
except Exception as e:
    print("erro: ", e)
    raise

from igem import ge, omics
from igem.omics.models import GeneMap

# Specify the Ensembl release version (e.g., GRCh38)
# ensembl = EnsemblRelease(99)

# Specify the Ensembl release version (e.g., GRCh37)
ensembl = EnsemblRelease(75)

# Define the chromosome and position you want to query
chromosome = "1"
position = 787028

# Query for genes at the given position
genes = ensembl.genes_at_locus(chromosome, position)

if genes:
    # Extract gene information
    for gene in genes:
        print(f"Gene ID: {gene.gene_id}, Gene Name: {gene.gene_name}")
        gene_simbol = str({gene.gene_name}).lower()

        # TODO: criar uma regra para buscar do gene name o term e do term os links

        # Get the term ID
        v_term = GeneMap.objects.filter(symbol=gene_simbol)
        print(v_term)
    

else:
    print("No genes found at this position.")
