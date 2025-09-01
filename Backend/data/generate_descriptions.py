import glob
import json
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv
from langchain_core.prompts import PromptTemplate

from Backend import models
from transformtion_code import code_dict

load_dotenv()

def get_metadata(collection):
    with open(f"Catalogs/{collection}.yml") as stream:
        catalog = yaml.safe_load(stream)

    res ={}
    for item in catalog:
        res[item] = catalog[item]["description"]

    return res


def fromatInput(frame, max_row):
    titles = ",".join(frame.columns.tolist())
    values = frame.iloc[0:max_row].values
    res = []
    for i in range(len(values)):
        row = [str(elem) for elem in values[i]]
        row = ",".join(row)
        res.append(row + "\n")

    return titles, res


def generateCollection(collection_name):
    sys_prompt = json.load(open("prompts.json"))['collection_system_prompt']

    metadata = get_metadata(collection_name)
    llm = models.get_LLM()

    input_prompt = """The data set describtions for the collection {collection} are:
                {descriptions}
            """

    def formatData(metaData):
        res = ""
        for k,v in metaData.items():
            res+= f"{k}:\n{v}\n"
        return res


    meta_data = formatData(metadata)
    input_prompt = input_prompt.format(collection=collection_name,descriptions=meta_data )
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    response = llm.invoke(messages)

    try:
        with open("../data/Catalogs/catalog.yml") as stream:
            catalog = yaml.safe_load(stream)
            catalog[collection_name]["description"] = response.content
            with open(f"../data/Catalogs/catalog.yml", 'w') as yaml_file:
                yaml.dump(catalog, yaml_file, default_flow_style=False)
    except FileNotFoundError:
        print("could not find the main catalog")
        return
    except KeyError:
        print("Could not find the collection. Wont proceed")
        return


def generateText(titles, values, file_name):
    llm = models.get_LLM()

    sys_prompt = json.load(open("prompts.json"))['text_system_prompt']

    input_prompt = PromptTemplate.from_template(json.load(open("prompts.json"))['data_input_prompt'])
    input_prompt = input_prompt.format(titles=titles, values=values, file=file_name)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages)

def _generate_product(collection,product):

    with open(f"Catalogs/{collection}.yml") as stream:
        catalog = yaml.safe_load(stream)

    file = f"{collection}/{product}.csv"

    if file == "california_schools\schools.csv":
        df = pd.read_csv(file, dtype={"CharterNum": str})
    elif file == "card_games\cards.csv":
        df = pd.read_csv(file,
                         dtype={"duelDeck": str, "flavorName": str, "frameVersion": str, "loyalty": str,
                                "originalReleaseDate": str})
    elif file == r"financial\trans.csv":
        df = pd.read_csv(file, dtype={"bank": str})
    else:
        df = pd.read_csv(file, index_col=0)

    df = df.round(3)

    titles, values = fromatInput(df, 2)
    catalog[product]["description"] = generateText(titles, values, Path(file).stem).content

    with open(f"Catalogs/{collection}.yml", 'w') as yaml_file:
        yaml.dump(catalog, yaml_file, default_flow_style=False)


def generate_products(collection,product):
    files = glob.glob(f"{collection}/*.csv")

    for file in files:
        if product is not None:
            if product.lower() in file.lower():
                _generate_product(collection,product)
        else:
            _generate_product(collection,product)


def _generateFunctionFullCode(name,func,code):
    llm = models.get_LLM()

    sys_prompt = """ Your task is to summarize a function in 1 to 2 sentences, Focus on what the goal of the function is. Tailor the description to each function
    You are given a short textual description,its paramters and the functions python code. Do not focus on code details such as input classes rather describe what happens
    If no code is given focus on the availabe information
    Alwyas start the description with the function name givven as input not the name of the python function """

    input_prompt = """I would like a description for this function : {func} 
    The corresponding python code is: {code}
    It is called {name}"""
    input_prompt = input_prompt.format(func=func, name=name,code=code)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages)


def generate_function_descriptions():
    res =[]

    with open("Catalogs/transformation_catalog.yml") as stream:
        catalog = yaml.safe_load(stream)

    for func in catalog:
        catalog[func].pop('description', None)


    for k,v in code_dict.items():

        res_dict = {k:_generateFunctionFullCode(k,catalog[k],v).content}
        res.append(res_dict)

    json.dump(res, open("metadata_transformations.json", 'w'))


def _create_column_description(format, description):
    llm = models.get_structured_LLM()

    sys_prompt = """ Your task is to provide a short description, as short as possible
    Include data format, especially if it is a secific format such as a url or a date format
    The output should be a valid json with
    "description": description of the what kind of data it is"""

    input_prompt = """I would like a description for this data: 
        {format}
        {description}
        """
    input_prompt = input_prompt.format(format=format, description=description)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages)

def create_column_description(collection_name, product_name ):
    with open(f"Catalogs/{collection_name}.yml") as stream:
        collection = yaml.safe_load(stream)

    for k in collection.keys():
        columns = collection[k]['columns']
        new = []
        if product_name is None or product_name == k:
            with open(f"{collection_name}/database_description/{k}.csv", 'r') as file:
                des = file.read().strip().split("\n")[1:]

                for d, c in zip(des, columns):
                    res = _create_column_description(list(c.values())[0],d)

                    new.append({d.split(",")[0].strip():f"{res['description']}"})

            collection[k]["columns"] = new
        with open(f"Catalogs/{collection_name}.yml", 'w') as yaml_file:
            yaml.dump(collection, yaml_file, default_flow_style=False)




if __name__ == "__main__":
    pass