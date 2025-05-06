import numpy as np


def _rangeFilter(df, column, range_dict):
    if ('min' in range_dict) & ('max' in range_dict):
        mask = df[column].ge(range_dict['min']) & df[column].le(range_dict['max'])
    elif 'min' in range_dict:
        mask = df[column].ge(range_dict['min'])
    elif 'max' in range_dict:
        mask = df[column].le(range_dict['max'])

    return df[mask]


def applyFilter(df, filter_dict):
    for key, val in filter_dict.items():
        if isinstance(val, dict):
            df = _rangeFilter(df, key, val)
        elif isinstance(val, list):
            idx, msg = _matchValues(df, key, val)
            df = df[idx]
        else:
            try:
                if isinstance(val, str):
                    val = val.lower()
                    df = df[df[key].str.lower() == val]
                else:
                    df = df[df[key] == val]
            except KeyError:
                msg = f"Could not find coulumn {column} in the dataset please try to rephrase your query"
    return df


def _matchValues(df, column, values):
    idx = []
    msg = ""

    for val in values:
        if isinstance(val,str):
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

    return idx#, msg

def _getInDf(df,column):
    pass



def getRows(df, filter_dict):
    msg = ""
    for column, values in filter_dict.items():
        if column in df:
            if (values is not None) and (values != "None"):

                idx, msg = _matchValues(df, column, values)
                df = df[idx]
            else:
                try:
                    df = df[column]
                except KeyError:
                    msg = f"Could not find coulumn {column} in the dataset please try to rephrase your query"
        else :
            df = df[values]
    return df#, msg


def combineProducts(df, filter_dict):
    for column, values in filter_dict.items():
        if not isinstance(values):
            values = [values]
        df = df[df[column].isin(values)]

    return df
