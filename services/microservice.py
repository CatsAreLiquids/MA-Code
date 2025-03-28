import pandas as pd
import requests
import os
from flask import Flask, jsonify, request
import ast

import util

app = Flask(__name__)
port = int(os.environ.get('PORT', 5000))

func_dict ={'sum':util.placeholder,"average":util.placeholder,"absolute":util.placeholder,"relative":util.placeholder}



@app.route("/")
def home():
    return "Hello, this is a Flask Microservice"

@app.route('/products/EDGAR_2024_GHG',methods=['GET'])
def getProduct():
    file = request.args.get('file')
    df = pd.read_csv('../data/data_products/'+file+'.csv')

    return df.to_json()

@app.route('/products/EDGAR_2024_GHG/filter',methods=['GET'])
def getSum():
    file = request.args.get('file')
    filter_dict = ast.literal_eval(request.args.get('filter_dict'))
    func = request.args.get('func')

    df = pd.read_csv('data/data_products/'+file+'.csv')
    df = util.applyFilter(df,filter_dict)

    if func:
        df = filter_dict[func](df)

    return df.to_json()







if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=port)