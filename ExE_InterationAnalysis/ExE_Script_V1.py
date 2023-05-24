"""
PROCESSO 1: QUERY
1 - Entrar com uma lista de NHAME ID e NHAME Descriction --> VarDescription
2 - Buscar pela NHAMES Description todas os Terms relacionados igem.ge.filter.word_to_term()  --> Normalizations
3 - Buscar para todos os Terms os registros em TermMap --> Keylink

PROCESSO 2: QC
1 - No Keylink deixar apenas os Terms
2 - Na Normalizations deixar apenas o NHAMES Description e o Term
    3 - Eliminar terms que iniciam com 'anat', 'go', 'path', 'meta:hmd0002111 '
    4 - Eliminar registos sem terms
    5 - Eliminar registros com NHAMES Description duplicados (qual seria o criterio para escolher qual term fica?)
6 - Da Keylink, mapeas os terms com o NHAMES Description.
    7 - manter apenas registros em que ambos terms foram mapeados com o NHAMES Description
8 - Na VarDescription em que temos apenas o ID e Decription, eliminas as duplicidades
9 - Passar tudo para UPPER CASE (Vardescription quanto os KeyLink)
10 - Ligar da KeyLink com varDescription com o objetivo de manter apenas ps NHAMES ID ao inves dos Terms --> gepairs


Part 1: Query
-------------
CRIAR UM UNICO PROCESSO DE BUSCA
1 - Entrada de uma Lista de ID e Descriptions
2 - Rodar word_to_term para as Descriptions
3 - Pesquisar os TermMap 
4 - Super table (ID, Description, Word, Term, Term_1, Term_2, ID_1, ID_2, Groups/Categories, counts)

com essa super table, o usuario pode realizar a limpeza conforme a sua necessidade, eliminando assim categorias que nao sao de interesse


Part 2: ExE LRT ANALYSY
-----------------------


"""


import pandas as pd
import scipy
import statsmodels.api as sm
from sklearn.linear_model import LinearRegression

try:
    import os
    import sys
    from pathlib import Path

    import pandas as pd

    v_root = Path(__file__).parents[2]
    sys.path.append(os.path.abspath(v_root))
except Exception as e:
    print("erro: ", e)
    raise

from igem.epc import load

# Data Path
v_load = v_root / "_utils" / "jiayan_analysis" / "data"


# CARGA DOS DADOS
# dados originados do WORD TO TERM
normalization = load.from_csv(str(v_load / "Normalization.csv"))
# consulta dos TERM encontrados na TERMMAP, para Term_1 e Term_2
keylink = load.from_csv(str(v_load / "keylink.csv"))
# ID and Desc NHAMES
VarDescription = load.from_csv(str(v_load / "VarDescription.csv"))


# ISOLAR COLUNAS DE DADOS TARGETS
# Process to find pairs
pairID = keylink[["keyge_1", "keyge_2"]].copy()
normalization_short = normalization[["Fatores", "keyge"]].copy()


# ELIMINAR DADOS QUE NAO SAO TARGETS
for x in ["anat", "go", "path", "meta:hmdb0002111"]:
    normalization_short = normalization_short[
        ~normalization_short["keyge"].astype(str).str.startswith(x)
    ]


# ELIMINAR DADOS INCONSISTENTES
normalization_clean = normalization_short.dropna()
# aqui o Jiayan eliminou os fatores duplicados, eliminando aqui TERMS
normalization_clean = normalization_clean.drop_duplicates(subset="Fatores", keep="last")

print(normalization_clean)
print(pairID)

# MERGE TABLES:
# RETORNA AS LIGACOES EM QUE TEMOS TANTO KEYGE1 QUANTO KEYGE2 NA NORMALIZATIONS_CLEAN
pairMap = pairID.merge(
    normalization_clean, left_on="keyge_1", right_on="keyge", how="left"
)
pairMap = pairMap.merge(
    normalization_clean, left_on="keyge_2", right_on="keyge", how="left"
)
normalization_short = normalization[["Fatores", "keyge"]].copy()
pairMap = pairMap.dropna()
pairMap2 = pairMap[["keyge_1", "keyge_2", "Fatores_x", "Fatores_y"]].copy()


VarDesc_Short = VarDescription[["var", "var_desc"]].copy()
VarDesc_clean = VarDesc_Short.drop_duplicates(subset="var_desc", keep="last")


pairMap2["Fatores_x"] = pairMap2["Fatores_x"].str.upper()
pairMap2["Fatores_y"] = pairMap2["Fatores_y"].str.upper()
VarDesc_clean["var_desc"] = VarDesc_clean["var_desc"].str.upper()

ToNAHNESID = pairMap2.merge(
    VarDesc_clean, left_on="Fatores_x", right_on="var_desc", how="left"
)
ToNAHNESID = ToNAHNESID.merge(
    VarDesc_clean, left_on="Fatores_y", right_on="var_desc", how="left"
)

NAHNESID = ToNAHNESID[["var_desc_x", "var_desc_y"]]

NAHNESID = NAHNESID.dropna()

# NAHNESID.to_csv(str(v_load / "gepairs.csv"))


# -----------------------------------------------------
# START REGRESSION PART
# Regression and LRTs with ExE pairs in R

gepairs = NAHNESID  # or read the file


MainTable = load.from_csv(str(v_load / "MainTable.csv"))


# sera que podemos utilizar o CLARITE para a regressao??
# library(lmtest)


# funcao para eliminar os NA
# completeFun <- function(data, desiredCols) {
#   completeVec <- complete.cases(data[, desiredCols])
#   return(data[completeVec, ])
# }

# no gepairs temos apenas UPPER CASE??
remove = [
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

# Elimina a linha que tenha alguma dessas string, porem na lista temos valores lower e o arquivo so tem UPPER
# remove rows that contain any string in the vector in the team column
# gepairs = gepairs[!grepl(paste(remove, collapse='|'), gepairs$NHANESID_var1),]
# gepairs = gepairs[!grepl(paste(remove, collapse='|'), gepairs$NHANESID_var2),]


for i in remove:
    gepairs = gepairs[~gepairs["var_desc_x"].str.contains(i)]
    gepairs = gepairs[~gepairs["var_desc_y"].str.contains(i)]

gepairs = gepairs.reset_index()
...

## HEMOGLOBIN
resultstable_dis = pd.DataFrame()
resultstable_rep = pd.DataFrame()


nested_table = MainTable.loc[
    :,
    [
        "LBXHGB",
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
    ],
]

# for i in range(len(gepairs)):
for i in range(1):
    e1 = gepairs.loc[i][1]
    e2 = gepairs.loc[i][2]
    nested_table["e1"] = e1
    nested_table["e2"] = e2
    nested_table = nested_table.fillna(0)
    nested_table_dis = nested_table[nested_table["SDDSRVYR"].isin([1, 2])]

    complex_table = nested_table
    complex_table["interaction"] = complex_table["e1"] + complex_table["e2"]
    complex_table_dis = nested_table[nested_table["SDDSRVYR"].isin([1, 2])]

    # Regression
    y1 = nested_table["LBXHGB"]
    X1 = nested_table[
        [
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
            "e1",
            "e2",
        ]
    ]
    X1 = sm.add_constant(X1)
    nested = sm.OLS(y1, X1).fit()
    nested_ll = nested.llf
    print(nested_ll)

    y2 = complex_table["LBXHGB"]
    X2 = complex_table[
        [
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
            "e1",
            "e2",
            "interation",
        ]
    ]
    X2 = sm.add_constant(X2)
    complex = sm.OLS(y2, X2).fit()
    complex_ll = complex.llf
    print(complex_ll)

    # TODO: podemos compactar e utilizar apenas uma tabela de dados de entrada

    # STEP 3: Perform the Log-Likelihood Test
    # Next, we’ll use the following code to perform the log-likelihood test:
    # calculate likelihood ratio Chi-Squared test statistic
    LR_statistic = -2 * (nested_ll - complex_ll)
    print(LR_statistic)

    # calculate p-value of test statistic using 2 degrees of freedom
    p_val = scipy.stats.chi2.sf(LR_statistic, 2)

    print(p_val)

    resultstable_dis[i, 1] = p_val
    resultstable_dis[i, 2] = e1
    resultstable_dis[i, 3] = e2
