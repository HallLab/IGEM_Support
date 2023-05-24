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

try:
    v_root = Path(__file__).parents[2]
    sys.path.append(os.path.abspath(v_root))
except Exception as e:
    print("erro: ", e)
    raise

from igem import epc, ge

# Data Path
v_path = v_root / "scripts" / "ExE_InterationAnalysis" / "files"

# ------------------------------------------------------------
# STEP 1: Get IGEM Terms from a ID and Description NHAMES List
# ------------------------------------------------------------

df_nhames_desc = epc.load.from_csv(str(v_path / "nhanes_description.csv"))
df_nhames_desc = epc.modify.colfilter(df_nhames_desc, only=["var", "var_desc"])
df_nhames_desc["var_desc"].to_csv(
    str(v_path / "nhanes_description_igem.csv"), index=False
)

# From NHAMES get IGEM Terms
# df_ge_terms = ge.filter.word_to_term(str(v_path /
#   "nhanes_description_strings.csv"))
df_terms = epc.load.from_csv(
    str(v_path / "nhanes_description_terms.csv"), index_col=False
)
df_terms = epc.modify.colfilter(
    df_terms, only=["fatores", "term"]
)  # TODO: change output format


# ------------------------------------------------------------
# STEP 2: Get TermMap from Terms founds in step 1
# ------------------------------------------------------------

# Delete Terms that is not target study
for x in ["anat", "go", "path", "meta:hmdb0002111"]:
    df_terms = df_terms[~df_terms["term"].astype(str).str.startswith(x)]
df_terms = df_terms.dropna()
df_terms = df_terms.drop_duplicates(subset="fatores", keep="last")

# Return NHANES ID to df_terms
df_terms["fatores"] = df_terms["fatores"].str.lower()
df_nhames_desc["var_desc"] = df_nhames_desc["var_desc"].str.lower()
df_terms = df_terms.merge(
    df_nhames_desc, left_on="fatores", right_on="var_desc", how="left"
)

# Convert DF column term to Terms List
list_term = df_terms["term"].tolist()

# Get all Term Map from Terms List
df_term_map = ge.filter.term_map(term=list_term)

# Clear fields from Term Map DF
df_term_map = epc.modify.colfilter(df_term_map, only=["term_1", "term_2"])


# ------------------------------------------------------------
# STEP 3: Replace Terms ID by NHANES ID
# ------------------------------------------------------------

# Replace Terms by NHANES ID on df_term_map
# TODO: I could use epc.modify.merge_variables because we don`t have to key
df_nhanes_map = df_term_map.merge(
    df_terms, left_on="term_1", right_on="term", how="left"
)
df_nhanes_map = df_nhanes_map.merge(
    df_terms, left_on="term_2", right_on="term", how="left"
)

df_nhanes_map = epc.modify.colfilter(
    df_nhanes_map, only=["var_x", "var_desc_x", "var_y", "var_desc_y"]
)


# ------------------------------------------------------------
# STEP 4: Clean df_nhanes_map to match the study target
# ------------------------------------------------------------

# Delete maps without one of the nhanes ID
df_nhanes_map = df_nhanes_map.dropna()

# Define list of nhanes ID that will not match the study target
list_remove = [
    "pneu",
    "current_asthma",
    "EVER",
    "any",
    "ATORVASTATIN",
    "AZITHROMYCIN",
    "CARVEDILOL",
    "hepb",
    "FENOFIBRATE",
    "FLUOXETINE",
    "BUPROPION",
    "GLYBURIDE",
    "ASPIRIN",
    "heroin",
    "ALENDRONATE",
    "METFORMIN",
    "ESTRADIOL",
    "OMEPRAZOLE",
    "NIFEDIPINE",
    "PREDNISONE",
    "PIOGLITAZONE",
    "ROFECOXIB",
    "ALBUTEROL",
    "SPIRONOLACTONE",
    "SIMVASTATIN",
    "SERTRALINE",
    "LOVASTATIN",
    "LOSARTAN",
    "cocaine",
    "DIGOXIN",
    "CELECOXIB",
]
for i in list_remove:
    df_nhanes_map = df_nhanes_map[~df_nhanes_map["var_x"].str.contains(i)]
    df_nhanes_map = df_nhanes_map[~df_nhanes_map["var_y"].str.contains(i)]

# fix the df index
df_nhanes_map = df_nhanes_map.reset_index()
