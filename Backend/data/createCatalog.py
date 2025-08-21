import pandas as pd
from pathlib import Path
import yaml
import requests
import os
import glob
from Backend import models


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


def _get_or_create(columns, api, collection, product):
    if _collection_exists(collection):
        try:
            with open(f"Catalogs/{collection}.yml") as stream:
                data = yaml.safe_load(stream)
        except FileNotFoundError:
            print("File exists but error")
    else:
        data = {}

    data[product] = {"columns": columns, "base_api": api}
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


def process_product(file):
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


def main(file):
    process_Product(file)


def identify_columns():
    llm = models.get_LLM()

    sys_prompt = """ Your task is to rewrite a user query into an sql query.
                """
    input_prompt = PromptTemplate.from_template("""
                        User Query:{query}
                        """)
    input_prompt = input_prompt.format(query=query)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]

    return llm.invoke(messages)



if __name__ == '__main__':
    files = glob.glob("toxicology/*.csv")
    for file in files:
        print(file)
        process_product(file)
    #file = "old/EDGAR_2023_GHG/GHG_per_capita_by_country_2023.csv"
    #main(file)
