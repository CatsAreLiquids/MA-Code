import ast

import pandas as pd


def getSum(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)
    if "group_by" in filter_dict:
        df = df.groupby(by=filter_dict['group_by'])

    if 'columns' in filter_dict:
        key = 'columns'
    elif 'column' in filter_dict:
        key = 'column'
    return df[filter_dict[key]].sum()


def mean(df, filter_dict):

    if "group_by" in filter_dict:
        df = df.groupby(by=filter_dict['group_by'])

    return df[filter_dict['column']].mean()


def combineProducts(first, second, filter_dict):

    filter_dict = ast.literal_eval(filter_dict)

    first = first.join(second, on=filter_dict['column'])

    if filter_dict['type'] != 'equals':
        if isinstance(second, pd.Series):
            values = second.unique().tolist()
        else:
            values = second[filter_dict['column']].unique().tolist()

        first = first[~first[filter_dict['column']].isin(values)]
    return first
