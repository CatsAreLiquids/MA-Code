import ast
import json
import pandas as pd
import requests
import io

from Backend.Agent.transformations import aggregation
from Backend.Agent.transformations import filter

aggregations = {'sum': aggregation.getSum, "mean": aggregation.mean}
filters = {'getRows': filter.getRows, 'filter': filter.applyFilter}


def getData(url):
    response = requests.get(url)
    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    return df


def _getDataProduct(url):
    """
    :param agent_result:
    :return:
    """
    try:
        return getData(url)
    except:
        return "could not access data product is the URL correct ?"


# TODO manage when data is to big
def _putDataProduct(df, function):
    response = requests.put(function["function"],
                            json={"data": df.to_json(), "args": json.dumps(function['values'])},
                            headers=function['values'])
    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    return df


def _executeProcessing(df, plan):
    for elem in plan:
        df = _putDataProduct(df, elem)
    return df


def execute(plan):
    frames = {}

    for i in range(len(plan)):
        df = _getDataProduct(plan[i]["product"])
        df = _executeProcessing(df, plan[i]["transformation"])
        frames["df_" + str(i)] = df

    return df
