import ast
import json
import pandas as pd
import requests
import io
import numpy as np

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
            df
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


def _executeProcessing(df, plan):
    for elem in plan:
        df = _putDataProduct(df, elem)
    return df

def executeStep(plan):

    retrieve = plan[0]
    df = getData(retrieve['values']['product'],retrieve['values']['columns'])

    for elem in plan[1:]:
        df = _putDataProduct(df, elem)

    return df


def execute(agent_result):
    plan = agent_result['products']
    combination = agent_result['combination']
    frames = {}

    for i in range(len(plan)):
        df = _getDataProduct(plan[i]["product"])
        if isinstance(df,str):
            return None
        df = _executeProcessing(df, plan[i]["transformation"])
        frames["df_" + str(i)] = df

    if len(plan) -1 != len(combination): #& len(combination[0]) != 0:
        return frames

    previous = frames["df_0"]
    for i in range(len(combination)):
        new = frames["df_"+str(i+1)]
        previous = _putDataProductCombination(previous,new,combination[i])

    return previous



def execute_new(agent_result):
    plans = agent_result['plans']

    frames = {}
    i = -1
    name = ""
    for elem in plans:
        print(elem)
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
        #print(frames["df_" + str(i)])
        try:
            tmp = frames["df_" + str(i)]
            print(tmp["DisplayName"].unique())
        except:
            pass

    return frames["df_" + str(i)]["df"]

if __name__ == "__main__":

    l ={'plans': [
        {'function': 'http://127.0.0.1:5200/retrieve', 'filter_dict': {'product': 'http://127.0.0.1:5000/products/superhero/gender'}},
        {'function': 'http://127.0.0.1:5200/retrieve', 'filter_dict': {'product': 'http://127.0.0.1:5000/products/superhero/superhero'}},
        {'function': 'combination', 'filter_dict': {'columns_left': 'id', 'columns_right': 'gender_id', 'type': 'equals', 'source_left': 'http://127.0.0.1:5000/products/superhero/gender', 'source_right': 'http://127.0.0.1:5000/products/superhero/superhero'}},
        {'function': 'http://127.0.0.1:5200/retrieve', 'filter_dict': {'product': 'http://127.0.0.1:5000/products/superhero/hero_power'}},
        {'function': 'combination', 'filter_dict': {'columns_left': 'id', 'columns_right': 'hero_id', 'type': 'equals', 'source_left': 'http://127.0.0.1:5000/products/superhero/superhero', 'source_right': 'http://127.0.0.1:5000/products/superhero/hero_power'}},
        {'function': 'http://127.0.0.1:5200/retrieve', 'filter_dict': {'product': 'http://127.0.0.1:5000/products/superhero/superpower'}},
        {'function': 'combination', 'filter_dict': {'columns_left': 'power_id', 'columns_right': 'id', 'type': 'equals', 'source_left': 'http://127.0.0.1:5000/products/superhero/hero_power', 'source_right': 'http://127.0.0.1:5000/products/superhero/superpower'}}]}


    t={'plans': [
        {'function': 'http://127.0.0.1:5200/retrieve',
         'filter_dict': {'product': 'http://127.0.0.1:5000/products/codebase_community/users'}},
{'function': 'http://127.0.0.1:5200/retrieve', 'filter_dict': {'product': 'http://127.0.0.1:5000/products/codebase_community/posts'}},
{'function': 'http://127.0.0.1:5200/filter', 'filter_dict': {'conditions': {'Title': 'Understanding what Dassault iSight is doing?'}}},
 {'function': 'combination', 'filter_dict': {'columns_left': 'Id', 'columns_right': 'OwnerUserId', 'type': 'equals'}}]}





    tmp =execute_new(l)
    tmp = tmp.reindex(sorted(tmp.columns), axis=1)
    print("result",execute_new(l))