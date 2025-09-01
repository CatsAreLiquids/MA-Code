import ast

import numpy as np
import pandas as pd


def _rangeFilter(df, column, range_dict):
    if ('min' in range_dict) & ('max' in range_dict):
        try:
            mask = df[column].ge(range_dict['min']) & df[column].le(range_dict['max'])
        except TypeError:
            mask = df[column].ge(int(range_dict['min'])) & df[column].le(int(range_dict['max']))
    elif 'min' in range_dict:
        try:
            mask = df[column].ge(range_dict['min'])
        except TypeError:
            mask = df[column].ge(int(range_dict['min']))
    elif 'max' in range_dict:
        try:
            mask = df[column].le(range_dict['max'])
        except:
            mask = df[column].le(int(range_dict['max']))
    return df[mask]

def _range_or(df, column, or_list):
    idx = []
    for elem in or_list:
        if 'min' in elem:
            mask = df[column].ge(elem['min'])
            idx += np.where(np.asarray(mask))[0].tolist()

        if 'max' in elem:
            mask = df[column].le(elem['max'])
            idx += np.where(np.asarray(mask))[0].tolist()

    return df.iloc[idx]

def applyFilter(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)


    if 'conditions' in filter_dict:
        filter_dict = filter_dict['conditions']
    elif 'condition' in filter_dict:
        filter_dict = filter_dict['condition']
    elif "criteria" in filter_dict:
        filter_dict = filter_dict['criteria']
    elif 'columns' in filter_dict:
        filter_dict = filter_dict['columns']

    if isinstance(filter_dict,list):
        filter_dict = filter_dict[0]

    for key, val in filter_dict.items():
        if key == "columns" :
            return df
        elif isinstance(val, dict):
            if list(val.keys())[0] == 'not_null':
                if  list(val.values())[0] == 'True':
                    df = df.dropna(subset=[key])
                    return df
                else:
                    mask = df[key].isna()
                    return df[mask]
            df = _rangeFilter(df, key, val)
        elif isinstance(val, list):
            if isinstance(val[0],dict):
                df = _range_or(df, key, val)
            else:
                df =  _matchValues(df, key, val)
        else:
            try:
                if isinstance(val, str):
                    if val == 'max':
                        return getMax(df,{'columns':key})
                    elif val =="empty":
                        df = np.where(df[key].isna()==True)[0].tolist()

                    elif val =="not empty":
                        df = df.dropna(subset=[key])
                    else:
                        val = val.lower()
                        try:
                            df = df[df[key].str.lower() == val]
                        except AttributeError:
                            df[key]= df[key].astype(str)
                            df = df[df[key].str.lower() == val]
                else:
                    df = df[df[key] == val]
            except KeyError:
                return df
    return df


def _matchValues(df, column, values):
    idx = []

    for val in values:
        mask = df[column] == val
        tmp = np.where(np.asarray(mask))[0].tolist()
        idx += tmp

    return df.iloc[idx]

def getMax(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)

    if 'column' in filter_dict:
        key = 'column'
    else:
        key = 'columns'
    if isinstance(df, pd.DataFrame):
        try:
            if "rows" in filter_dict:
                return df.nlargest(n=filter_dict["rows"],columns=filter_dict[key])
        except TypeError:
            print(df[filter_dict[key]].astype("float"))
            df[filter_dict[key]] = pd.to_numeric(df[filter_dict[key]])
            return df.nlargest(n=filter_dict["rows"], columns=filter_dict[key])
    else:
        if isinstance(df, pd.Series):
            df = df.to_frame()
            return {"value":str(df.max()[0]),filter_dict[key]:df.idxmax()[0]}
        try:
            return df[df[filter_dict[key]].idxmax()]
        except KeyError:
            return df

def getMin(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)

    if 'column' in filter_dict:
        key = 'column'
    else:
        key = 'columns'

    if "rows" in filter_dict:
        return df.nsmallest(filter_dict["rows"],filter_dict[key])
    else:
        if isinstance(df, pd.Series):
            df = df.to_frame()
            return {"value":str(df.max()[0]),filter_dict[key]:df.idxmin()[0]}
        try:
            return df[df[filter_dict[key]].idxmin()]
        except KeyError:
            return df


def getRows(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)
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
