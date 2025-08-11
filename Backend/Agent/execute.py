import ast
import json
import pandas as pd
import requests
import io


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
    for elem in plans:
        #print(elem)
        if elem['function'] == 'http://127.0.0.1:5200/retrieve':
            df = getData(elem['filter_dict'])
            i += 1
            frames["df_" + str(i)] = df
        elif elem['function'] == "combination":
            previous = frames["df_" + str(i - 1)]
            new = frames["df_" + str(i)]
            df = _putDataProductCombination(previous, new, elem['filter_dict'])

            i += 1
            frames["df_" + str(i)] = df

        else:
            df = _putDataProduct(df, elem)
            frames["df_" + str(i)] = df
        #print(frames["df_" + str(i)])
        try:
            tmp = frames["df_" + str(i)]
            print(tmp["DisplayName"].unique())
        except:
            pass

    return frames["df_" + str(i)]

if __name__ == "__main__":

    l ={'plans': [
{'function': 'http://127.0.0.1:5200/retrieve', 'filter_dict': {'product': 'http://127.0.0.1:5000/products/formula_1/driverStandings'}},
 {'function': 'http://127.0.0.1:5200/filter', 'filter_dict': {'conditions': {'year': {'min': 1980, 'max': 1985}}}},
 {'function': 'http://127.0.0.1:5200/retrieve', 'filter_dict': {'product': 'http://127.0.0.1:5000/products/formula_1/pitStops'}},
 {'function': 'http://127.0.0.1:5200/mean', 'filter_dict': {'group_by': None, 'columns': ['duration']}},
 {'function': 'http://127.0.0.1:5200/max', 'filter_dict': {'columns': 'duration', 'rows': 3}}]}







    print("result",execute_new(l))