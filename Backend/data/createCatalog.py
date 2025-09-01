import glob
import sys
from pathlib import Path

import pandas as pd
import requests
import yaml

from Backend import models
from Backend.data import generate_descriptions  as generate
from Backend.RAG import vector_db

baseAPi = "http://127.0.0.1:5000/products"


def _collection_exists(collection):
    path = Path(f"Catalogs/{collection}.yml")
    return path.exists()


def _data_exists(collection,product):
    path = Path(f"{collection}/{product}")
    return path.exists()


def _testAPI(url):
    response = requests.get(url)
    if response.status_code != 200:
        return False
    else:
        return True

def format_columns(columns):
    for i in range(len(columns)):
        columns[i] = {columns[i]: columns[i]}
    return columns

def get_products(collection):
    with open(f"Catalogs/catalog.yml") as stream:
        catalog = yaml.safe_load(stream)
        try:
            catalog = catalog[collection]
        except KeyError:
            print("Could not find specified collection")

    return catalog["products"]

def create_in_meta_catalog(collection):
    if _collection_exists("catalog"):
        try:
            with open(f"Catalogs/catalog.yml") as stream:
                data = yaml.safe_load(stream)
        except FileNotFoundError:
            print("File exists but error")
    else:
        data = {}

    data[collection] = {"description": "", "name": collection,"products": get_products(collection),}
    with open(f"Catalogs/catalog.yml", 'w') as yaml_file:
        yaml.dump(data, yaml_file, default_flow_style=False)


def _get_or_create(columns, api, collection, product):
    if _collection_exists(collection):
        try:
            with open(f"Catalogs/{collection}.yml") as stream:
                data = yaml.safe_load(stream)
        except FileNotFoundError:
            print("File exists but error")
    else:
        data = {}

    data[product] = {"columns": format_columns(columns), "base_api": api,"description": ""}
    with open(f"Catalogs/{collection}.yml", 'w') as yaml_file:
        yaml.dump(data, yaml_file, default_flow_style=False)


def _get_columns(file):

    if file == "california_schools\schools.csv":
        df = pd.read_csv(file, dtype={"CharterNum": str})
    elif file == "card_games\cards.csv":
        df = pd.read_csv(file,
                         dtype={"duelDeck": str, "flavorName": str, "frameVersion": str, "loyalty": str,
                                "originalReleaseDate": str})
    elif file == r"financial\trans.csv":
        df = pd.read_csv(file,dtype={"bank":str})
    else:
        df = pd.read_csv(file)
    return df.columns.tolist()

def _process_product(file):
    collection = Path(file).parts[0]
    product = Path(file).parts[1]

    if _data_exists(collection,product):
        columns = _get_columns(file)
    else:
        print("Could not find the specified data product. Not going to proceed")
        return

    api = baseAPi + f"/{collection}/{Path(file).stem}"
    if not _testAPI(api):
        print(f"error on API call make sure the data product ({api}) is callable. Not going to proceed")
        return

    _get_or_create(columns, api, collection, Path(product).stem)

def create_catalog(collection,product_name):
    files = glob.glob(f"{collection}/*.csv")

    for file in files:
        if product_name is not None:
            if product_name.lower() in file.lower():
                _process_product(file)

        else:
            _process_product(file)

    create_in_meta_catalog(collection)


def enrich_catalog(collection,product_name):
    files = glob.glob(f"{collection}/*.csv")

    for file in files:
        if product_name is not None:
            if product_name.lower() in file.lower():
                generate.create_column_description(collection,product_name)
                generate.generate_products(collection,product_name)
        else:
            generate.create_column_description(collection,product_name)
            generate.generate_products(collection, product_name)


    generate.generateCollection(collection)



if __name__ == '__main__':
    create_catalog("toxicology","atom")
