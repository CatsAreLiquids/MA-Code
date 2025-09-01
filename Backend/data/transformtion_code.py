
_sum =  """def getSum(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)
    if "group_by" in filter_dict:
        df = df.groupby(by=filter_dict['group_by'])

    if 'columns' in filter_dict:
        key = 'columns'
    elif 'column' in filter_dict:
        key = 'column'
    return df[filter_dict[key]].sum()"""
_mean = """def mean(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)
    if "group_by" in filter_dict:
        df = df.groupby(by=filter_dict['group_by'])

    return df[filter_dict['columns']].mean().reset_index()"""
_max = """def getMax(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)

    if 'column' in filter_dict:
        key = 'column'
    else:
        key = 'columns'

    if "rows" in filter_dict:
        return df.nlargest(filter_dict["rows"],filter_dict[key])
    else:
        if isinstance(df, pd.Series):
            df = df.to_frame()
            return {"value":str(df.max()[0]),filter_dict[key]:df.idxmax()[0]}
        try:
            return df[df[filter_dict[key]].idxmax()]
        except KeyError:
            return df"""
_min = """def getMin(df, filter_dict):
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
            return df"""
_filter = """def applyFilter(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)

    if 'conditions' in filter_dict:
        filter_dict = filter_dict['conditions']
    elif 'columns' in filter_dict:
        filter_dict = filter_dict['columns']

    if isinstance(filter_dict,list):
        filter_dict = filter_dict[0]

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
                    elif val =="empty":
                        idx = np.where(df[key].isna()==True)[0].tolist()
                        df = df.loc[:,idx]
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
    return df"""
_sortby = """def sortby(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)

    order = True
    if 'ascending' in filter_dict:
        if isinstance(filter_dict['ascending'],bool):
            order = filter_dict['ascending']
        else:
            if 'ascending' == filter_dict['ascending']:
                order = True
            elif 'descending' == filter_dict['ascending']:
                order = False
    if isinstance(df,pd.Series):
        return df.sort_values(ascending=order)
    else:
        try:
            return df.sort_values(by=[filter_dict['columns']],ascending=order)
        except:
            return df"""
_count = """def count(df, filter_dict):
    filter_dict = ast.literal_eval(filter_dict)
    if 'columns' in filter_dict:
        key = 'columns'
    elif 'column' in filter_dict:
        key = 'column'


    if "group_by" in filter_dict:
        df_group = df.groupby(by=filter_dict['group_by'])
    try:
        df_group = df_group[filter_dict[key]].agg('count').rename(columns={filter_dict[key][0]: 'count'}).reset_index()
    except TypeError:
        df_group = df_group[filter_dict[key]].agg('count').rename("count").to_frame().reset_index()
    return df_group"""
_returnResult = """def call_return():
    content = json.loads(request.data)

    return {'data': content}"""
_retrieve = """def getData(func_dict):
    url = func_dict["product"]
    if "column" in func_dict:
        columns = func_dict["column"]
    else: columns = None


    response = requests.get(url)
    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    if columns is not None:
        try:
            df = df[columns]
        except KeyError:
            df
    return df"""
_combine = """def combineProducts(first, second, filter_dict):
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

    return first"""

code_dict = {"sum":_sum,"mean":_mean, "max":_max,"min":_min,"filter":_filter,
             "sortby":_sortby,"count":_count,"returnResult":_returnResult,
             "retrieve":_retrieve,"combine":_combine}