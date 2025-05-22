import pandas as pd
import os
from flask import Flask, request
import yaml
import json

app = Flask(__name__)
port = int(os.environ.get('PORT', 5000))


@app.route('/catalog', methods=['GET'])
def getCatalog():
    content = json.loads(request.data)
    file = content['file']

    if "/" in file:
        file = file.split("/")[-1]

    try:
        with open("../data/Catalogs/catalog.yml") as stream:
            catalog = yaml.safe_load(stream)
    except FileNotFoundError:
        return "could not find the main catalog"

    for collection in catalog:
        if file in collection['products']:
            try:
                with open("../data/Catalogs/" + collection['name'] + ".yml") as stream:
                    collection_dict = yaml.safe_load(stream)
                    print(file)
                    return collection_dict[file]
            except FileNotFoundError:
                return "could not find the specific collection catalog"
            except KeyError:
                return collection_dict

@app.route('/catalog/columns', methods=['GET'])
def getCatalogColumns():
    print(request.data)
    content = json.loads(request.data)
    file = content['file']

    if "/" in file:
        file = file.split("/")[-1]

    try:
        with open("../data/Catalogs/catalog.yml") as stream:
            catalog = yaml.safe_load(stream)
    except FileNotFoundError:
        return "could not find the main catalog"

    for collection in catalog:
        if file in collection['products']:
            try:
                with open("../data/Catalogs/" + collection['name'] + ".yml") as stream:
                    collection_dict = yaml.safe_load(stream)
                    print(file)
                    return collection_dict[file]['columns']
            except FileNotFoundError:
                return "could not find the specific collection catalog"
            except KeyError:
                return collection_dict

@app.route('/catalog/EDGAR_2024_GHG', methods=['GET'])
def getCatalogEDGAR_2024_GHG():
    file = request.args.get('file')
    try:
        with open("../dataCatalog/Catalogs/" + file + ".yml") as stream:
            return yaml.safe_load(stream)
    except FileNotFoundError:
        return "Could not find a catalog item asociated to your request"


@app.route('/catalog/Sales_Data', methods=['GET'])
def getCatalogSales_Data():
    file = request.args.get('file')
    try:
        with open("../dataCatalog/Catalogs/" + file + ".yml") as stream:
            return yaml.safe_load(stream)
    except FileNotFoundError:
        return "Could not find a catalog item asociated to your request"


@app.route('/products/EDGAR_2024_GHG/GHG_by_sector_and_country', methods=['GET'])
def getProduct1():
    file = request.args.get('file')
    # read from data catalog
    df = pd.read_csv('../data/EDGAR_2024_GHG/GHG_by_sector_and_country.csv')
    return {'data': df.to_json()}


@app.route('/products/EDGAR_2024_GHG/GHG_totals_by_country', methods=['GET'])
def getProduct6():
    file = request.args.get('file')
    # read from data catalog
    df = pd.read_csv('../data/EDGAR_2024_GHG/GHG_totals_by_country.csv')
    return {'data': df.to_json()}


@app.route('/products/EDGAR_2024_GHG/GHG_per_capita_by_country', methods=['GET'])
def getProduct7():
    file = request.args.get('file')
    # read from data catalog
    df = pd.read_csv('../data/EDGAR_2024_GHG/GHG_per_capita_by_country.csv')
    return {'data': df.to_json()}


@app.route('/products/EDGAR_2024_GHG/GHG_per_GDP_by_country', methods=['GET'])
def getProduct5():
    file = request.args.get('file')
    # read from data catalog
    df = pd.read_csv('../data/EDGAR_2024_GHG/GHG_per_GDP_by_country.csv')
    return {'data': df.to_json()}


@app.route('/products/EDGAR_2024_GHG/LULUCF_macroregions', methods=['GET'])
def getProduct2():
    df = pd.read_csv('../data/EDGAR_2024_GHG/LULUCF_macroregions.csv')
    return {'data': df.to_json()}


@app.route('/products/Sales_Data/customer_data_23', methods=['GET'])
def getProduct3():
    df = pd.read_csv('../data/Sales_Data/customer_data_23.csv')
    return {'data': df.to_json()}


@app.route('/products/Sales_Data/sales_data_23', methods=['GET'])
def getProduct4():
    df = pd.read_csv('../data/Sales_Data/sales_data_23.csv')
    return {'data': df.to_json()}


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=port)
