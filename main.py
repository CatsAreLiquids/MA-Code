import json

import pandas as pd

if __name__ == "__main__":
    df = pd.read_excel("data/EDGAR_2024_GHG_booklet_2024.xlsx",sheet_name="LULUCF_macroregions")
    df = df.dropna(how='all')
    df.to_csv("data/EDGAR_2024_GHG/LULUCF_macroregions.csv", index=False)