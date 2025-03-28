import pandas as pd
import numpy as np


def yearFilter(columns,filter_dict):
    # TODO fucks up if we have real dates problem for later
    year_columns = [int(column) for column in columns if column.isdigit()]
    if ('min_year' in filter_dict) & ('max_year' in filter_dict):
        idx = np.where((np.asarray(year_columns) >= filter_dict['min_year']) &
                                (np.asarray(year_columns) <= filter_dict['max_year']))
    elif 'min_year' in filter_dict:
        idx = np.where(np.asarray(year_columns) >= filter_dict['min_year'])
    elif 'max_year' in filter_dict:
        idx = np.where(np.asarray(year_columns) <= filter_dict['max_year'])

    year_columns = [str(year_columns[i]) for i in idx[0]]

    return year_columns


def applyFilter(df,filter_dict):
    columns = df.columns.tolist()
    attr_keys= [*filter_dict]
    attr_keys = [key for key in attr_keys if key not in['min_year','max_year']]

    if ('min_year' in filter_dict) or ('max_year' in filter_dict):
        year_columns = yearFilter(columns,filter_dict)
    else:
        year_columns =[]

    df = df[attr_keys+year_columns]

    return df

def getSum(df):
    #TODO need to do proper preprocessing to untangle data and not have multi dimensional data or figure out namiing schema

    return None

def placeholder(df):
    print(df)

    return df
