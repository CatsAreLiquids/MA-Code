import ast
import json
import pandas as pd
import requests
import io


def getData(url,columns=None):
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
                            json={"data_1": first.to_json(),"data_2": second.to_json() ,"args": json.dumps(function)})
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

    print(plan)
    retrieve = plan[0]
    df = getData(retrieve['values']['product'],retrieve['values']['columns'])

    for elem in plan[1:]:
        print(elem)
        df = _putDataProduct(df, elem)
        print(df)
    return df


def execute(agent_result):
    plan = agent_result['products']
    combination = agent_result['combination']
    frames = {}

    for i in range(len(plan)):
        print(plan[i]["product"])
        df = _getDataProduct(plan[i]["product"])
        if isinstance(df,str):
            print(df)
            return None
        print(plan[i]["transformation"])
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
    combination = agent_result['combination']
    frames = {}
    i = 0
    for elem in plans:
        print(elem)
        if elem['function'] == 'http://127.0.0.1:5200/retrieve':
            df = getData(elem['values']['product'], elem['values']['columns'])
            i+=1
        else:
            df = _putDataProduct(df, elem)
        frames["df_" + str(i)] = df

    if len(plans) -1 != len(combination): #& len(combination[0]) != 0:
        return frames

    previous = frames["df_0"]
    for i in range(len(combination)):
        new = frames["df_"+str(i+1)]
        previous = _putDataProductCombination(previous,new,combination[i])

    return previous