import json
import requests
import pandas as pd
import io
import ast
import glob
import os




def retrieve_as_request_task(**kwargs):
    url = kwargs["filter_dict"]["product"]

    response = requests.get(url)
    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    df.to_csv(f"temp_storage/df_{kwargs['product_nr']}.csv", index=False)

def filter_as_request_task(**kwargs):

    df = pd.read_csv(f"temp_storage/df_{kwargs['product_nr']}.csv")

    args = json.dumps(kwargs['filter_dict'])
    response = requests.put("http://127.0.0.1:5200/filter",
                            json={"data": df.to_json(), "args": args})

    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    df.to_csv(f"temp_storage/df_{kwargs['product_nr']}.csv", index=False)

def min_as_request_task(**kwargs):

    df = pd.read_csv(f"temp_storage/df_{kwargs['product_nr']}.csv")

    args = json.dumps(kwargs['filter_dict'])
    response = requests.put("http://127.0.0.1:5200/min",
                            json={"data": df.to_json(), "args": args})

    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    df.to_csv(f"temp_storage/df_{kwargs['product_nr']}.csv", index=False)

def max_as_request_task(**kwargs):

    df = pd.read_csv(f"temp_storage/df_{kwargs['product_nr']}.csv")

    args = json.dumps(kwargs['filter_dict'])
    response = requests.put("http://127.0.0.1:5200/max",
                            json={"data": df.to_json(), "args": args})

    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    df.to_csv(f"temp_storage/df_{kwargs['product_nr']}.csv", index=False)

def sum_as_request_task(**kwargs):

    df = pd.read_csv(f"temp_storage/df_{kwargs['product_nr']}.csv")

    args = json.dumps(kwargs['filter_dict'])
    response = requests.put("http://127.0.0.1:5200/sum",
                            json={"data": df.to_json(), "args": args})

    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    df.to_csv(f"temp_storage/df_{kwargs['product_nr']}.csv", index=False)

def mean_as_request_task(**kwargs):

    df = pd.read_csv(f"temp_storage/df_{kwargs['product_nr']}.csv")

    args = json.dumps(kwargs['filter_dict'])
    response = requests.put("http://127.0.0.1:5200/mean",
                            json={"data": df.to_json(), "args": args})

    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    df.to_csv(f"temp_storage/df_{kwargs['product_nr']}.csv", index=False)

def sortby_as_request_task(**kwargs):

    df = pd.read_csv(f"temp_storage/df_{kwargs['product_nr']}.csv")

    args = json.dumps(kwargs['filter_dict'])
    response = requests.put("http://127.0.0.1:5200/sortby",
                            json={"data": df.to_json(), "args": args})

    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    df.to_csv(f"temp_storage/df_{kwargs['product_nr']}.csv", index=False)

def count_as_request_task(**kwargs):

    df = pd.read_csv(f"temp_storage/df_{kwargs['product_nr']}.csv")

    args = json.dumps(kwargs['filter_dict'])
    response = requests.put("http://127.0.0.1:5200/count",
                            json={"data": df.to_json(), "args": args})

    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    df.to_csv(f"temp_storage/df_{kwargs['product_nr']}.csv", index=False)

def returnResult_as_request_task(**kwargs):

    df = pd.read_csv(f"temp_storage/df_{kwargs['product_nr']}.csv")

    args = json.dumps(kwargs['filter_dict'])
    response = requests.put("http://127.0.0.1:5200/returnResult",
                            json={"data": df.to_json(), "args": args})

    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    df.to_csv(f"temp_storage/df_{kwargs['product_nr']}.csv", index=False)

def combination_as_request_task(**kwargs):

    left = pd.read_csv(f"temp_storage/df_{kwargs['left_product']}.csv")
    right = pd.read_csv(f"temp_storage/df_{kwargs['right_product']}.csv")

    args = json.dumps(kwargs['filter_dict'])
    response = requests.put('http://127.0.0.1:5200/combine',
                            json={"data_1": left.to_json(), "data_2": right.to_json(),
                                  "args": args})

    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    nr = int(kwargs["right_product"].split("_")[-1])+1
    dag_id = kwargs["right_product"].split("_")[0]

    df.to_csv(f"temp_storage/df_{dag_id}_{str(nr)}.csv", index=False)

def cleanup(**kwargs):
    dag_id = kwargs["dag_id"]

    def helper(file_name):
        file_name= file_name.split(".csv")[0]
        return int(file_name.split("_")[-1])

    files = glob.glob(f"temp_storage/df_{dag_id}_[0-9]*.csv")
    file = max(files, key=helper)

    os.rename(file, f"results/{dag_id}.csv")

    files = glob.glob(f"temp_storage/df_{dag_id}_[0-9]*.csv")
    for file in files:
        os.remove(file)


if __name__ == "__main__":
    pass