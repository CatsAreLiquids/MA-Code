import pandas as pd
import requests
import os
from flask import Flask, jsonify, request
import ast

import util

from retrieval import agentic

app = Flask(__name__)
port = int(os.environ.get('PORT', 5000))

func_dict = {'sum': util.getSum, "average": util.getMean, "absolute": util.getAbsoluteDiff,
             "relative": util.getRelativeDiff}


@app.route("/")
def home():
    return "Hello, this is a Flask Microservice"


@app.route('/products/EDGAR_2024_GHG', methods=['GET'])
def getProduct():
    file = request.args.get('file')
    df = pd.read_csv('../data/data_products/EDGAR_2024_GHG/' + file + '.csv')

    return {'data':df.to_json()}


@app.route('/products/EDGAR_2024_GHG/filter', methods=['GET'])
def getSum():
    args = request.args
    filter_dict = {}

    for key, value in args.items():
        if key == 'file':
            file = value
        elif key == 'func':
            func = value
        else:
            filter_dict[key] = value

    df = pd.read_csv('../data/data_products/EDGAR_2024_GHG/' + file + '.csv',index_col=[0])
    #TODO what happens if i dont have any filters
    df = util.applyFilter(df,filter_dict)

    #TODO mechanisim to defer data that cant done like this
    if func not in ["none","None"]:
        df = func_dict[func.lower()](df)

    return {'data':df.to_json()}


@app.route('/chat', methods=['GET'])
def forward_agent():
    message = request.args.get('message')
    if message is None:
        return "Please provide input"
    agent_result = agentic.remoteChat(message)
    res_dict = agentic.parseResult(agent_result['output'])

    if res_dict['success']:
        res_dict['message'],res_dict['urls'] = agentic.validateURL(res_dict['urls'])

    return {'success':res_dict['success'], 'urls':res_dict['urls'],
            'message':res_dict['message'],'data_name':res_dict['data_names']}


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=port)
