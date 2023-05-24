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

from igem import epc

# Data Path
v_path = v_root / "_utils" / "jiayan_analysis" / "files"


# ------------------------------------------------------------
# STEP 5: Define Main Table, Outcomes and Covariantes
# ------------------------------------------------------------

# DataFrame to collect results
df_results_discover_final = pd.DataFrame()
df_results_replicate_final = pd.DataFrame()
list_results_discover = []
list_results_replicate = []

# Read NHANES Main Table
df_maintable = epc.load.from_csv(str(v_path / "MainTable.csv"))

# list of Outcomes
list_outcome = [
    "LBXHGB",
]

# list of Covariants
list_covariant = [
    "female",
    "black",
    "mexican",
    "other_hispanic",
    "other_eth",
    "SDDSRVYR",
    "BMXBMI",
    "SES_LEVEL",
    "RIDAGEYR",
    "LBXCOT",
    "IRON_mg",
]


# ------------------------------------------------------------
# STEP 6: Run the ExE Interation by Outcome and Map Pairs
# ------------------------------------------------------------

# --> Start: DELETE THIS CODE ON FLY
# READ
df_nhanes_map = epc.load.from_csv(str(v_path / "gepairs.csv"), index_col=None)
df_nhanes_map = df_nhanes_map.rename(
    columns={"NHANESID_var1": "var_x", "NHANESID_var2": "var_y"}
)
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
df_nhanes_map = epc.modify.colfilter(df_nhanes_map, skip="ID")
# --> End: DELETE

# Run each Outcome in defined list
for i_outcome in list_outcome:
    # Run each pair map in df_nhanes_map
    for i_mappair in df_nhanes_map.index:
        # get Exposomes
        e1 = df_nhanes_map["var_x"][i_mappair]
        e2 = df_nhanes_map["var_y"][i_mappair]

        # Keep only the necessary columns to run Interations Study
        df_maintable_exe = df_maintable.loc[
            :, list_covariant + list_outcome + list([e1, e2])
        ]
        df_maintable_exe = df_maintable_exe.fillna(0)

        # Filter data table to Discovery Data
        df_maintable_exe = df_maintable_exe[
            df_maintable_exe["SDDSRVYR"].isin([1, 2])
            ]

        # Run Interation Study
        Interation_Study = epc.analyze.interaction_study(
            data=df_maintable_exe,
            outcomes=i_outcome,
            interactions=[(e1, e2)],
            covariates=list_covariant,
        )

        # Save results in list: outcome/e1/e2/converged/LRT_pvalue/Bonfp
        list_results_discover.append(
            [
                Interation_Study.LRT_pvalue.index.levels[2][0],
                Interation_Study.LRT_pvalue.index.levels[0][0],
                Interation_Study.LRT_pvalue.index.levels[1][0],
                Interation_Study.Converged.values[0],
                Interation_Study.LRT_pvalue.values[0],
                Interation_Study.LRT_pvalue.values[0] * len(df_nhanes_map),
            ]
        )

    # Transf result list in df with Bonfp < 0.05
    df_results_discover = pd.DataFrame(
        list_results_discover,
        columns=[
            "Outcome", "Term1", "Term2", "Converged", "LRT_pvalue", "Bonfp"
            ],
    )
    df_results_discover = df_results_discover.loc[
        df_results_discover["Bonfp"] <= 0.05
        ]

    # Run Replicate for all discovery result with Bonfp < 0.05
    for x in df_results_discover.itertuples(index=False):
        e1 = x.Term1
        e2 = x.Term2

        # Keep only the necessary columns to run Interations Study
        df_maintable_exe = df_maintable.loc[
            :, list_covariant + list_outcome + list([e1, e2])
        ]
        df_maintable_exe = df_maintable_exe.fillna(0)

        # Filter data table to Replicate Data
        df_maintable_exe = df_maintable_exe[
            df_maintable_exe["SDDSRVYR"].isin([3, 4])
            ]

        # Run Interation Study
        Interation_Study = epc.analyze.interaction_study(
            data=df_maintable_exe,
            outcomes=i_outcome,
            interactions=[(e1, e2)],
            covariates=list_covariant,
        )

        # Save results in list: outcome/e1/e2/converged/LRT_pvalue/Bonfp
        list_results_replicate.append(
            [
                Interation_Study.LRT_pvalue.index.levels[2][0],
                Interation_Study.LRT_pvalue.index.levels[0][0],
                Interation_Study.LRT_pvalue.index.levels[1][0],
                Interation_Study.Converged.values[0],
                Interation_Study.LRT_pvalue.values[0],
                Interation_Study.LRT_pvalue.values[0] * len(
                    df_results_discover
                ),
            ]
        )

    # Transf result list in df with Bonfp < 0.05
    df_results_replicate = pd.DataFrame(
        list_results_replicate,
        columns=[
            "Outcome", "Term1", "Term2", "Converged", "LRT_pvalue", "Bonfp"
            ],
    )
    df_results_replicate = df_results_replicate.loc[
        df_results_replicate["Bonfp"] <= 0.05
    ]

    df_results_discover_final = df_results_discover_final.append(
        df_results_discover
    ).reset_index(drop=True)
    df_results_replicate_final = df_results_replicate_final.append(
        df_results_replicate
    ).reset_index(drop=True)

print(df_results_discover_final)
print(df_results_replicate_final)
