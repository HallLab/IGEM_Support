# main.py
import os
import sys
from pathlib import Path

try:
    v_root = Path(__file__).parents[2]
    sys.path.append(os.path.abspath(v_root))
except Exception as e:
    print("error: ", e)
    raise

from igem import epc


def main():
    # Read NHANES Main Table
    current_path = Path.cwd()
    df_maintable = epc.load.from_csv(
        str(current_path / "scripts/ExE_InterationAnalysis/files/MainTable_sample.csv")  # noqa E501
    )

    # list of Outcomes
    list_outcome = [
        "LBXHGB",
    ]

    # list of Covariates
    list_covariant = [
        "female",
        "black",
        "mexican",
        "other_hispanic",
        "other_eth",
        "SES_LEVEL",
    ]

    # result = epc.analyze.exe_teste()
    result = epc.analyze.exe_pairwise(
        df_maintable,
        list_outcome,
        list_covariant,
        report_betas=False,
        process_num=4,
    )

    print(result)


if __name__ == '__main__':
    main()
