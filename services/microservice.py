import pandas as pd
import os
from flask import Flask, request
import yaml
import util

from agent import test

app = Flask(__name__)
port = int(os.environ.get('PORT', 5000))

func_dict = {'sum': util.getSum, "average": util.getMean, "absolute": util.getAbsoluteDiff,
             "relative": util.getRelativeDiff, 'ids': util.getIDs}

agent = agentic.init_agent()


@app.route('/catalog', methods=['GET'])
def getCatalog():
    try:
        with open("../dataCatalog/configs/catalog.yml") as stream:
            return yaml.safe_load(stream)
    except FileNotFoundError:
        return "Main catalog is not findable"


@app.route('/catalog/EDGAR_2024_GHG', methods=['GET'])
def getCatalog():
    file = request.args.get('file')
    try:
        with open("../dataCatalog/configs/" + file + ".yml") as stream:
            return yaml.safe_load(stream)
    except FileNotFoundError:
        return "Could not find a catalog item asociated to your request"


@app.route('/catalog/Sales_Data', methods=['GET'])
def getCatalog():
    file = request.args.get('file')
    try:
        with open("../dataCatalog/configs/" + file + ".yml") as stream:
            return yaml.safe_load(stream)
    except FileNotFoundError:
        return "Could not find a catalog item asociated to your request"


@app.route('/products/EDGAR_2024_GHG', methods=['GET'])
def getProduct():
    file = request.args.get('file')
    # read from data catalog
    df = pd.read_csv('../data/EDGAR_2024_GHG/' + file + '.csv')
    return {'data': df.to_json()}


@app.route('/products/EDGAR_2024_GHG/GHG_by_sector_and_country', methods=['GET'])
def getProduct():
    file = request.args.get('file')
    # read from data catalog
    df = pd.read_csv('../data/EDGAR_2024_GHG/GHG_by_sector_and_country.csv')
    return {'data': df.to_json()}


@app.route('/products/EDGAR_2024_GHG/LULUCF_macroregions', methods=['GET'])
def getProduct():
    df = pd.read_csv('../data/EDGAR_2024_GHG/LULUCF_macroregions.csv')
    return {'data': df.to_json()}

@app.route('/products/Sales_Data/customer_data_23', methods=['GET'])
def getProduct():
    df = pd.read_csv('../data/Sales_Data/customer_data_23.csv')
    return {'data': df.to_json()}

@app.route('/products/Sales_Data/sales_data_23', methods=['GET'])
def getProduct():
    df = pd.read_csv('../data/Sales_Data/sales_data_23.csv')
    return {'data': df.to_json()}


@app.route('/chat', methods=['GET'])
def forward_agent():
    message = request.args.get('message')
    id = request.args.get('id')
    if message is None:
        return "Please provide input"
    agent_result = agent.invoke({"input": message})
    res_dict = agentic.parseResult(agent_result['output'])

    if res_dict['success']:
        res_dict['message'], res_dict['urls'] = agentic.validateURL(res_dict['urls'])

    return {'success': res_dict['success'], 'urls': res_dict['urls'],
            'message': res_dict['message'], 'data_name': res_dict['data_names']}


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=port)
