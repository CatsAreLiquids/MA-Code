import pandas as pd
import numpy as np
import itertools

def GHG_totals_by_country():
    df = pd.read_excel('Original_Data/EDGAR_2024_GHG_booklet_2024.xlsx', sheet_name='GHG_totals_by_country')
    df.rename(columns={"Country": "year", }, inplace=True)
    df = df.drop(columns=["EDGAR Country Code"])
    df['Country']= df['Country'].str.lower()
    df = df.dropna()
    df = df.T
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])

    df.to_csv('./GHG_totals_by_country.csv')


# Need to seperate this into sector and substance type
def GHG_by_sector_and_country():
    df = pd.read_excel('./Original_Data/EDGAR_2024_GHG_booklet_2024.xlsx', sheet_name='GHG_by_sector_and_country')
    df = df.drop(columns=["EDGAR Country Code"])
    df = df.dropna(how='all')

    print(df.head())

    df.rename(columns={"Country": "year", }, inplace=True)
    df = df.T
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])
    #df.to_csv('./GHG_by_sector_and_country.csv')


def GHG_per_GDP_by_country():
    df = pd.read_excel('./Original_Data/EDGAR_2024_GHG_booklet_2024.xlsx', sheet_name='GHG_per_GDP_by_country')
    df.rename(columns={"Country": "year", }, inplace=True)
    df = df.dropna(how='all')
    df = df.drop(columns=["EDGAR Country Code"]).T
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])

    df.to_csv('./GHG_per_GDP_by_country.csv')


def GHG_per_capita_by_country():
    df = pd.read_excel('./Original_Data/EDGAR_2024_GHG_booklet_2024.xlsx', sheet_name='GHG_per_capita_by_country')
    df.rename(columns={"Country": "year", }, inplace=True)
    df = df.drop(columns=["EDGAR Country Code"])
    df = df.dropna()
    df = df.T
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])

    df.to_csv('./GHG_per_capita_by_country.csv')


def LULUCF_macroregions():
    df = pd.read_excel('./Original_Data/EDGAR_2024_GHG_booklet_2024.xlsx', sheet_name='LULUCF_macroregions')
    df = df.dropna(how='all')

    sectors = df['Sector'].unique()
    substances = df['Substance'].unique()


    for pair in itertools.product(sectors, substances):
        df_tmp = df[(df['Substance'] == pair[1]) & (df['Sector'] == pair[0])]
        df_tmp = df_tmp.drop(columns=['Substance', 'Sector'])
        df_tmp.rename(columns={"Macro-region": "year", }, inplace=True)
        df_tmp = df_tmp.T
        df_tmp.columns = df_tmp.iloc[0]
        df_tmp = df_tmp.drop(df_tmp.index[0])

        if not df_tmp.empty:
            df_tmp.to_csv(
                './LULUCF_macroregions_' + pair[1] + '_' + pair[0] + '.csv')




if __name__ == "__main__":
    GHG_totals_by_country()
    #GHG_by_sector_and_country()
    #GHG_per_GDP_by_country()
    #GHG_per_capita_by_country()
    #LULUCF_macroregions()
