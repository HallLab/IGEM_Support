"""
Part 1: Queries
-------------
CREATE A SINGLE SEARCH PROCESS
1 - Entry of a List of IDs and Descriptions
2 - Rotate word_to_term for the Descriptions
3 - Search the TermMap
4 - Super table (ID, Description, Word, Term, Term_1, Term_2, ID_1, ID_2,
    Groups/Categories, counts)

with this super table, the user can perform the cleaning as needed, thus
eliminating categories that are not of interest

Part 2: ExE LRT ANALYSY
-----------------------
5 - Define Main Table, Outcomes and Covariantes
6 -  Run the ExE Interation by Outcome and Map Pairs
"""

import os
import sys
from pathlib import Path

import pandas as pd

try:
    v_root = Path(__file__).parents[2]
    sys.path.append(os.path.abspath(v_root))
except Exception as e:
    print("erro: ", e)
    raise

import igem as igem
from igem import ge
from igem.epc import load, modify

# Convert String List to Terns
v_file = str(v_root) + "/scripts/echo_exposomes/echo_exposomes.csv"
check = ge.filter.word_to_term(path=v_file)
if check:
    print("file saved as output_word_to_term.csv")
else:
    pass # raise

# Read list of terms
# TODO: Add Mold
v_terms = load.from_csv(
    str(v_root) + "/scripts/echo_exposomes/output_word_to_term.csv"
    )
v_term = modify.colfilter(v_terms, only=["term"])
v_term = v_term.drop_duplicates()
v_term = v_term.dropna()
ls_term = list(v_term['term'])

df_term_map = ge.filter.term_map(term=ls_term)
df_term_map.to_csv(str(v_root) + "/scripts/echo_exposomes/term-term.csv")
print(df_term_map)


# ------------------------------------------------------------
# STEP X: Filter Terms that is not Exposomes
# ------------------------------------------------------------

# TODO: Clean 

# Clear fields from Term Map DF
df_term_map = igem.epc.modify.colfilter(df_term_map, only=["term_1", "term_2"])
df_term_map = df_term_map.drop_duplicates(keep="last")


# ------------------------------------------------------------
# STEP X: Link Terms ID by ECHO Description
# ------------------------------------------------------------
df_echo_map = df_term_map.merge(
    v_terms, left_on="term_1", right_on="term", how="left"
)
df_echo_map = df_echo_map.merge(
    v_terms, left_on="term_2", right_on="term", how="left"
)

df_echo_map = igem.epc.modify.colfilter(
    df_echo_map, only=["term_1", "string_x", "term_2", "string_y"]
)

pass