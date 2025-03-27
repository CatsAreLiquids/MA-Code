import pandas as pd
import numpy as np
from typing import List

#TODO fix indexing issue
def selectValues(df,codes,years:List):
    if codes:
        df = df[df['EDGAR Country Code'].isin(codes)]

    if years:
        cols = df.columns.tolist()[2:]

        if len(years) > 1: # We have two values one min year one max year
            cols = np.where((np.asarray(cols) >= years[0]) & (np.asarray(cols) <= years[1]))
        else: # we only have one year which is the max
            cols = np.where( (np.asarray(cols) <= years[0]))
        cols = cols[0]
        cols+=2

        cols = [0, 1] + cols.tolist()
        df = df.iloc[:, cols]

    return df

#TODO fix this for all versions
def createGrowth(codes,years:List):
    df = pd.read_excel('data/EDGAR_2024_GHG_booklet_2024.xlsx', sheet_name='GHG_totals_by_country')
    df = selectValues(df,codes,years)
    df = df.T

    for col in df.columns.tolist():
        col_name = df[col]['EDGAR Country Code']
        df[col_name+'_growth_abs'] =0
        df[col_name+'_growth_abs'] = df[col].iloc[2:] - df[col].iloc[2:].shift()

    df  = df.T
    df.iloc[len(codes):,[0,1]] = df[['EDGAR Country Code','Country']].iloc[0:len(codes)]
    df = df.drop(df.index[0:len(codes)])

    codes = '_'.join(code for code in codes)
    years = '-'.join(str(year) for year in years)

    df.to_csv('data/data_products/GHG_totals_by_country_'+codes+years+'.csv')





if __name__ == "__main__":
    codes = ["BEL", "BGR", "DNK", "DEU", "EST", "FIN", "FRA", "GRC", "IRL", "ITA",
                 "HRV", "LVA", "LTU", "LUX", "MLT", "NLD", "AUT", "POL", "PRT", "ROU",
                 "SWE", "SVK", "SVN", "ESP", "CZE", "HUN", "CYP"]
    codes = ["DEU", "AUT", "CHE"]
    df = pd.read_excel('data/EDGAR_2024_GHG_booklet_2024.xlsx', sheet_name='GHG_totals_by_country')
    df.reset_index(drop=True, inplace=True)
    df = df[df['EDGAR Country Code'].isin(codes)]
    df = createGrowth(["DEU"],[1995,2003])
    print(df)

    # df.to_csv("data/data_products/GHG_totals_by_country_DACH.csv")
