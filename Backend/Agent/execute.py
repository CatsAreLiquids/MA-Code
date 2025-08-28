import ast
import io
import json

import pandas as pd
import requests


def getData(func_dict):
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
            return df
    return df


def _putDataProduct(df, function):
    if 'values' in function:
        args = json.dumps(function['values'])
    if 'filter_dict' in function:
        args = json.dumps(function['filter_dict'])
    if 'columns' in function:
        args = json.dumps(function['columns'])
    response = requests.put(function["function"],
                            json={"data": df.to_json(), "args": args})

    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    return df

def _putDataProductCombination(first,second, function):

    response = requests.put('http://127.0.0.1:5200/combine',
                            json={"data_1": first["df"].to_json(), "data_1_name" : first["name"],
                                "data_2": second["df"].to_json() ,"data_2_name" : second["name"] ,"args": json.dumps(function)})

    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    return df


def execute(agent_result):
    plans = agent_result['plans']

    frames = {}
    i = -1
    name = ""
    for elem in plans:
        if elem['function'] == 'http://127.0.0.1:5200/retrieve':
            df = getData(elem['filter_dict'])

            i += 1

            name = elem['filter_dict']["product"].split("/")[-1]
            frames["df_" + str(i)] = {"df":df,"name":name}
        elif elem['function'] == "http://127.0.0.1:5200/returnResult" or elem['function'] == "returnResult":
            pass
        elif elem['function'] == "combination":
            previous = frames["df_" + str(i - 1)]
            new = frames["df_" + str(i)]
            df = _putDataProductCombination(previous, new, elem['filter_dict'])
            i += 1
            frames["df_" + str(i)] = {"df":df,"name":"combination"}

        else:
            df = _putDataProduct(df, elem)
            frames["df_" + str(i)] = {"df":df,"name":name}

        try:
            tmp = frames["df_" + str(i)]
        except:
            pass

    return frames["df_" + str(i)]["df"]

if __name__ == "__main__":
    """
    Use this code when testing functionality localy without having to generate airflow code:
    
    execute(plan)
    
    """
    pass