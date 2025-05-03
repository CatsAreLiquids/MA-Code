
def getSum(df, filter_dict):
    if "group_by" in filter_dict:
        df = df.groupby(by=filter_dict['group_by'])

    return df[filter_dict['column']].sum()

def mean(df):
    if "group_by" in filter_dict:
        df = df.groupby(by=filter_dict['group_by'])

    return df[filter_dict['column']].mean()


