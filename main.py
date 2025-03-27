import json

import pandas as pd

if __name__ == "__main__":
    codes = ["BEL", "BGR", "DNK", "DEU", "EST", "FIN", "FRA", "GRC", "IRL", "ITA",
             "HRV", "LVA", "LTU", "LUX", "MLT", "NLD", "AUT", "POL", "PRT", "ROU",
             "SWE", "SVK", "SVN", "ESP", "CZE", "HUN", "CYP"]
    #codes = ["DEU", "AUT", "CHE"]
    df = pd.read_excel('data/EDGAR_2024_GHG_booklet_2024.xlsx', sheet_name='GHG_totals_by_country')
    df = df[df['EDGAR Country Code'].isin(codes)]
    df
    # df.to_csv("data/data_products/GHG_totals_by_country_DACH.csv")
    print(len(df))

    val = """"Your task is top help by creating {count} general tags based on the provided data. \n Additionally if possible provide one tag for the start of the time series, as the year, as well as one for the end of the time series,as the year,. \n    \n    The provided data will be in the format of column titles and values.\n    The first row will be the titles: title1, title2, title3,...\n    The next rows will be the values: value1, value2, value 3,...\n   \n    Please provide {count} generall tags, such as the time range and topic of the data\n    \n    The tags are used to generally describe the data set. \n    The format should look like [["tag1", "tag2", "tag3",..],earliest,latest]"""
    data = {"key": val}
    #json.dump(data, open("./tmp.json", 'w'))

    df = pd.read_csv('data/data_products/GHG_totals_by_country_DEU1995-2003.csv')
    print(df.to_json())