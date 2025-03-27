import pandas as pd
import requests
import os
from flask import Flask, jsonify, request
app = Flask(__name__)
port = int(os.environ.get('PORT', 5000))

@app.route("/")
def home():
    return "Hello, this is a Flask Microservice"

@app.route('/products',methods=['GET'])
def getProduct():
    file = request.args.get('file')
    df = pd.read_csv('data/data_products/'+file+'.csv')

    return df.to_json()

@app.route('/filter',methods=['POST'])
def getProduct():
    file = request.args.get('file')
    df = pd.read_csv('data/data_products/'+file+'.csv')

    return df.to_json()







if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=port)