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
    filter_dict = ast.literal_eval(filter_dict)
    if "group_by" in filter_dict:
        df = df.groupby(by=filter_dict['group_by'])
        return df[filter_dict['columns']].mean().reset_index()
    else:
        return df[filter_dict['columns']].mean()


def count(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)
    if 'columns' in filter_dict:
        key = 'columns'
    elif 'column' in filter_dict:
        key = 'column'

    if "unique" in filter_dict :
        if (filter_dict["unique"] ==  "True") or filter_dict["unique"]:
            df = df.drop_duplicates(subset=filter_dict[key])

    if "group_by" in filter_dict:
        df_group = df.groupby(by=filter_dict['group_by'])
        try:
            df_group = df_group[filter_dict[key]].agg('count').rename(columns={filter_dict[key][0]: 'count'}).reset_index()
        except TypeError:
            df_group = df_group[filter_dict[key]].agg('count').rename("count").to_frame().reset_index()
        return df_group
    else:
        return df[filter_dict[key]].count()


def combineProducts(first, second, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)
    if isinstance(first, pd.DataFrame) and isinstance(second, pd.DataFrame):
        if ("columns_left" in filter_dict) and ("columns_right" in filter_dict):
            first = first.merge(second, left_on=filter_dict["columns_left"], right_on=filter_dict["columns_right"],suffixes=["","_y"])
            first.drop(first.filter(regex='_y$').columns, axis=1, inplace=True)

        else:
            first = first.join(second, on=filter_dict['column'])
    else:
        try:
            values = second[filter_dict['columns_right']].values().unique().tolist()
        except AttributeError:
            values = second[filter_dict['columns_right']]
            if type(values) != first[filter_dict['columns_left']].dtypes:
                values = [str(values)]
                first[filter_dict['columns_left']] = first[filter_dict['columns_left']].astype(str)

        first = first[first[filter_dict['columns_left']].isin(values)]

    if filter_dict['type'] != 'equals':
        if isinstance(second, pd.Series):
            values = second.unique().tolist()
        else:
            values = second[filter_dict['column']].unique().tolist()

        first = first[~first[filter_dict['column']].isin(values)]
    print(first)
    return first

