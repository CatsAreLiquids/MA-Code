import numpy as np
import ast
import pandas as pd


def getNRows(df,filter_dict):

    filter_dict = ast.literal_eval(filter_dict)
    if filter_dict['top'] == True:
        return df.head(filter_dict['n'])
    elif filter_dict['top'] == False:
        return df.tail(filter_dict['n'])
    else:
        return df


def _rangeFilter(df, column, range_dict):
    if ('min' in range_dict) & ('max' in range_dict):
        mask = df[column].ge(range_dict['min']) & df[column].le(range_dict['max'])
    elif 'min' in range_dict:
        mask = df[column].ge(range_dict['min'])
    elif 'max' in range_dict:
        mask = df[column].le(range_dict['max'])

    return df[mask]


def applyFilter(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)
    if 'columns' in filter_dict:
        filter_dict = filter_dict['columns']

    for key, val in filter_dict.items():
        if isinstance(val, dict):
            df = _rangeFilter(df, key, val)
        elif isinstance(val, list):
            idx, msg = _matchValues(df, key, val)
            df = df[idx]
        else:
            try:
                if isinstance(val, str):
                    if val == 'max':
                        return getMax(df,{'columns':key})
                    val = val.lower()
                    df = df[df[key].str.lower() == val]
                else:
                    df = df[df[key] == val]
            except KeyError:
                df
    return df


def _matchValues(df, column, values):
    idx = []
    msg = ""

    for val in values:
        if isinstance(val, str):
            val = val.lower()
        mask = df[column] == val
        tmp = np.where(np.asarray(mask))[0].tolist()
        if not tmp:
            if msg == "":
                msg = "could not find "
            msg += f"{val}, "

        idx += tmp

    if msg != "":
        msg += f"in the {column} column"

    return idx  # , msg


def _getInDf(df, column):
    pass


def getMax(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)
    if isinstance(df, pd.Series):
        df = df.to_frame()
        return {"value":str(df.max()[0]),filter_dict['column']:df.idxmax()[0]}
    try:
        return df[filter_dict['column']].max()
    except KeyError:
        return df


def getRows(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)
    print(filter_dict)
    if filter_dict['columns'] in df:
        if (filter_dict['values'] is not None) and (filter_dict['values'] != "None"):

            idx, msg = _matchValues(df, filter_dict['columns'], filter_dict['values'])
            return df[idx]
        else:
            try:
                return df[filter_dict['columns']]
            except KeyError:
                return df
    else:
        return df


def combineProducts(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)
    for column, values in filter_dict.items():
        if not isinstance(values):
            values = [values]
        df = df[df[column].isin(values)]

    return df
