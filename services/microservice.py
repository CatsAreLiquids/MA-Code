import pandas as pd
import os
from flask import Flask, request
import yaml
import util

from retrieval import agentic

app = Flask(__name__)
port = int(os.environ.get('PORT', 5000))

func_dict = {'sum': util.getSum, "average": util.getMean, "absolute": util.getAbsoluteDiff,
             "relative": util.getRelativeDiff}

agent = agentic.init_agent()

@app.route("/")
def home():
    return "Hello, this is a Flask Microservice"

@app.route('/catalog/EDGAR_2024_GHG', methods=['GET'])
def getCatalog():
    file = request.args.get('file')
    try:
        with open("../dataCatalog/configs/"+file+".yml") as stream:
            return yaml.safe_load(stream)
    except FileNotFoundError :
       return "Could not find a catalog item asociated to your request"





@app.route('/products/EDGAR_2024_GHG', methods=['GET'])
def getProduct():
    file = request.args.get('file')
    #read from data catalog
    df = pd.read_csv('../data/EDGAR_2024_GHG/' + file + '.csv')

    return {'data':df.to_json()}


@app.route('/products/EDGAR_2024_GHG/sum', methods=['GET'])
#todo redo this for row wise column wise
def getSum():
    args = request.args
    filter_dict = {}
    math_dict = {}
    for key, value in args.items():
        if key == 'file':
            file = value
        elif key in ['func','rolling','period']:
            math_dict[key] = value
        else:
            filter_dict[key] = value

    df = pd.read_csv('../data//EDGAR_2024_GHG/' + file + '.csv',index_col=[0],header=[0])
    #TODO what happens if i dont have any filters
    df = util.applyFilter(df,filter_dict)
    print(math_dict)
    #TODO mechanisim to defer data that cant done like this
    if math_dict['func'] not in ['none','None']:
        df = func_dict[math_dict['func'].lower()](df,math_dict['rolling'],math_dict['period'] )

    return {'data':df.to_json()}


@app.route('/chat', methods=['GET'])
def forward_agent():
    message = request.args.get('message')
    id = request.args.get('id')
    if message is None:
        return "Please provide input"
    agent_result = agent.invoke({"input": message})
    res_dict = agentic.parseResult(agent_result['output'])

    if res_dict['success']:
        res_dict['message'],res_dict['urls'] = agentic.validateURL(res_dict['urls'])

    return {'success':res_dict['success'], 'urls':res_dict['urls'],
            'message':res_dict['message'],'data_name':res_dict['data_names']}


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=port)
