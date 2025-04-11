import pandas as pd
import numpy as np


def yearFilter(dates, filter_dict):
    # TODO fucks up if we have real dates problem for later

    min_year = filter_dict.get('min_year')
    max_year = filter_dict.get('max_year')

    if (min_year is not None) & (max_year is not None):
        idx = np.where((np.asarray(dates) >= int(min_year)) &
                       (np.asarray(dates) <= int(max_year)))
    elif min_year is not None:
        idx = np.where(np.asarray(dates) >= int(min_year))
    elif max_year is not None:
        idx = np.where(np.asarray(dates) <= int(max_year))

    return idx


def applyFilter(df, filter_dict):
    columns = df.columns.tolist()
    attr_keys = [item for key, item in filter_dict.items() if key not in ['min_year', 'max_year','max_value','min_value']]

    if ('min_year' in filter_dict) or ('max_year' in filter_dict):
        if 'date' in columns:
            dates = df['date'].values.to_list()
        else:
            dates = df.index.values.tolist()

        date_rows = yearFilter(dates, filter_dict)
        df = df.iloc[date_rows]

    if ('min_value' in filter_dict) or ('max_value' in filter_dict):
        pass

    df = df[attr_keys]
    return df


#TOdo the reset index is kinda ugly
def getSum(df):
    return df.sum().reset_index()

def getMean(df):
    return df.mean().reset_index()

#TODO needs testing
def getAbsoluteDiff(df):
    return df.diff().reset_index()

#periods= x allows to calculate via diffrent periods
def getRelativeDiff(df):
    return df.pct_change().reset_index()

def placeholder(df):


    return df
