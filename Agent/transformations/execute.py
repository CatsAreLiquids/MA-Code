
def _getDataProduct(agent_result):
    """
    :param agent_result:
    :return:
    """
    try:
        return util.getData(agent_result['url'])
    except:
        return "could not access data product is the URL correct ?"


def _combineProducts(first, second, column, type, value):
    if type == "select":
        if (value is None) or (value==['None']):
            value = second[column].to_list()
        first = first[first[column].isin(value)]
    if type == "join":
        first = first.join(second, on=column)
        if value is not None:
            first = first[first[column].isin(value)]
    return first

def _executeBlocks(df, plan):

    #try:
    if isinstance(plan,str):
        plan = ast.literal_eval(plan)
    for elem in plan:
        if 'values' in elem:
            values = elem['values']
        else:
            values = None
        df = applyFunction(df, elem['function'], values)
    #except KeyError:

    return df


def execute(plan):
    if 'combine' in plan:


        try:
            column = plan['combine']['column']
            type = plan['combine']['type']
            values = plan['combine']['values']
        except KeyError:
            column = plan['column']
            type = plan['type']
            values = plan['values']

        df2 = _getDataProduct(plan['combine']['p2'][0])
        print("df2 pre",df2)
        df2 = _executeBlocks(df2, plan['combine']['p2'][1])

        print("df2 post",df2)

        df = _getDataProduct(plan['combine']['p1'][0])
        df = _combineProducts(df, df2, column, type, values)

        df = _executeBlocks(df, plan['combine']['p1'][1])


    else:
        df = _getDataProduct(plan['execute']['p1'][0])
        df = _executeBlocks(df, plan['execute']['p1'][1])

    return df


def applyFunction(df, function, values):
    if function in aggregations:
        return aggregations[function](df, values)
    if function in filters:
        return filters[function](df, values)

    return df