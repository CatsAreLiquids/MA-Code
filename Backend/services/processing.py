import os
from flask import Flask, request
import pandas as pd
import json
import io
import yaml
import ast
from Backend.services.transformations import aggregation
from Backend.services.transformations import filters
from Backend.services.transformations import misc
app = Flask(__name__)
port = int(os.environ.get('PORT', 5200))


@app.route('/catalog', methods=['GET'])
def getCatalog():
    content = json.loads(request.data)

    try:
        with open("../data/Catalogs/function_catalog.yml") as stream:
            catalog = yaml.safe_load(stream)
    except FileNotFoundError:
        return "could not find the main catalog"
    try:
        return catalog[content['function_name']]
    except KeyError:
        return catalog

@app.route('/sum', methods=['PUT'])
def call_sum():

    content = json.loads(request.data)

    df = pd.read_json(io.StringIO(content['data']))
    df = aggregation.getSum(df, content['args'])

    return {'data':df.to_json()}

@app.route('/max', methods=['PUT'])
def call_max():
    content = json.loads(request.data)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))
    df = filters.getMax(df, content['args'])

    if isinstance(df,pd.Series) or isinstance(df,pd.DataFrame):
        return {'data': df.to_json()}
    else:
        return {'data': json.dumps(df)}

@app.route('/combine', methods=['PUT'])
def call_combine():
    content = json.loads(request.data)

    try:
        df1 = pd.read_json(io.StringIO(content['data_1']))
    except ValueError:
        df1 = pd.Series(ast.literal_eval(content['data_1']))

    try:
        df2 = pd.read_json(io.StringIO(content['data_2']))
    except ValueError:
        df2 = pd.Series(ast.literal_eval(content['data_2']))

    df = aggregation.combineProducts(df1,df2, content['args'])

    {'data': df.to_json()}

@app.route('/getRows', methods=['PUT'])
def call_rows():
    content = json.loads(request.data)

    df = pd.read_json(io.StringIO(content['data']))
    df = filters.getRows(df, content['args'])

    return {'data': df.to_json()}

@app.route('/mean', methods=['PUT'])
def call_mean():
    content = json.loads(request.data)

    df = pd.read_json(io.StringIO(content['data']))
    df = aggregation.mean(df, content['args'])

    return {'data': df.to_json()}

@app.route('/filter', methods=['PUT'])
def call_filter():
    content = json.loads(request.data)

    df = pd.read_json(io.StringIO(content['data']))
    df = filters.applyFilter(df, content['args'])

    return {'data': df.to_json()}

@app.route('/sortby', methods=['PUT'])
def call_sortby():
    content = json.loads(request.data)

    df = pd.read_json(io.StringIO(content['data']))
    df = misc.sortby(df, content['args'])

    return {'data': df.to_json()}

@app.route('/getNRows', methods=['PUT'])
def call_getNRows():
    content = json.loads(request.data)

    df = pd.read_json(io.StringIO(content['data']))
    df = filters.getNRows(df, content['args'])

    return {'data': df.to_json()}


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=port)