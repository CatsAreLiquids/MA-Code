import ast


def getSum(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)
    if "group_by" in filter_dict:
        df = df.groupby(by=filter_dict['group_by'])

    return df[filter_dict['column']].sum()

def mean(df):
    if "group_by" in filter_dict:
        df = df.groupby(by=filter_dict['group_by'])

    return df[filter_dict['column']].mean()

def _combineProducts(first, second, column, type, value):

    if type == "select":
        if (value is None) or (value==['None']):
            if isinstance(second,pd.Series):
                value = second.tolist()
            else:
                value = second[column].to_list()
        first = first[first[column].isin(value)]
    if type == "join":
        first = first.join(second, on=column)
        if value is not None:
            first = first[first[column].isin(value)]
    return first

