import pandas as pd
import numpy as np
import ast


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


# Specifically on cell level assumes compareble dta so ints etc
def valueFilter(df, filter_dict):
    min_value = ast.literal_eval(filter_dict.get('min_value'))
    max_value = ast.literal_eval(filter_dict.get('max_value'))

    if (min_value is not None) & (max_value is not None):
        mask = df.ge(min_value) & df.le(max_value)
    elif min_value is not None:
        mask = df.ge(min_value)
    elif max_value is not None:
        mask = df.le(max_value)

    df = df[mask]
    df = df.dropna(how='all')
    return df


# TODO what todo if we reurn an empty df
def applyFilter(df, filter_dict):
    columns = df.columns.tolist()
    attr_keys = [item for key, item in filter_dict.items() if
                 key not in ['min_year', 'max_year', 'max_value', 'min_value']]

    if ('min_year' in filter_dict) or ('max_year' in filter_dict):
        if 'date' in columns:
            dates = df['date'].values.to_list()
        else:
            dates = df.index.values.tolist()

        date_rows = yearFilter(dates, filter_dict)
        df = df.iloc[date_rows]

    df = df[attr_keys]

    if ('min_value' in filter_dict) or ('max_value' in filter_dict):
        df = valueFilter(df, filter_dict)

    return df


# Todo the reset index is kinda ugly
def getSum(df, rolling, period):
    return df.sum().reset_index()


def getMean(df, rolling, period):
    return df.mean().reset_index()


# TODO needs testing
def getAbsoluteDiff(df, rolling, period):
    return df.diff().reset_index()


# periods= x allows to calculate via diffrent periods
def getRelativeDiff(df, rolling, period):
    return df.pct_change().reset_index()


def getIDs(df):
    return df


def placeholder(df):
    return df
