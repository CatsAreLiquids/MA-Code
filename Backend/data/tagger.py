import random

from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from dotenv import load_dotenv
import json
import os
import pandas as pd
import re
from langchain_core.prompts import PromptTemplate
import glob
from pathlib import Path
import time
import ast
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from Backend import models
import yaml
from dotenv import load_dotenv
import yaml
import numpy as np
from test import code_dict
from random import sample
import time
load_dotenv()



def openCatalog():

    try:
        with open("../data/Catalogs/catalog.yml") as stream:
            catalog = yaml.safe_load(stream)["collection_name"]
    except FileNotFoundError:
        print("could not find the main catalog")
        return
    except KeyError:
        print("Could not find the collection. Wont proceed")
        return

def manual_tagger(df):
    columns = df.columns.tolist()
    columns = [x for x in columns if not x.isdigit()]
    df_tmp = df[columns].copy()
    df_tmp = df_tmp.drop(columns=['EDGAR Country Code'])

    tags = [df[column].drop_duplicates().tolist() for column in df_tmp]
    return [tag for tag_types in tags for tag in tag_types]


def inferPeriod(df):
    min = df.index.min()
    max = df.index.max()


    val1 = is_datetime(min) & is_datetime(max)
    val2 = (bool(re.search(r"[0-9]{4}", str(min))) & bool(re.search(r"[0-9]{4}", str(max))))
    if val1 or val2:
        return (min,max)
    else:
        return None


def fromatInput(frame, max_row):
    titles = ",".join(frame.columns.tolist())
    values = frame.iloc[0:max_row].values
    res = []
    for i in range(len(values)):
        row = [str(elem) for elem in values[i]]
        row = ",".join(row)
        res.append(row + "\n")

    return titles, res


def generateTags(titles, values, file_name, tagNumber):
    sys_prompt = PromptTemplate.from_template(json.load(open("../../preprocessing/A/prompts.json"))['tagger_system_prompt'])
    sys_prompt = sys_prompt.format(count=tagNumber)
    input_prompt = PromptTemplate.from_template(json.load(open("../../preprocessing/A/prompts.json"))['data_input_prompt'])
    input_prompt = input_prompt.format(titles=titles, values=values, file=file_name)

    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages)


def generateCollection(collection_name):
    sys_prompt = json.load(open("../../preprocessing/A/prompts.json"))['collection_system_prompt']
    meta_data = json.load(open(f"{collection_name}/metadata_automatic.json"))
    llm = models.get_LLM()

    input_prompt = """The data set describtions for the collection {collection} are:
                {descriptions}
            """

    def formatData(metaData):
        res = ""
        for i in metaData:
            res+= f"{i}:\n{metaData[i]['description']}\n"
        return res

    meta_data = formatData(meta_data)
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
    sys_prompt = json.load(open("../../preprocessing/A/prompts.json"))['text_system_prompt']
    input_prompt = PromptTemplate.from_template(json.load(open("../../preprocessing/A/prompts.json"))['data_input_prompt'])
    input_prompt = input_prompt.format(titles=titles, values=values, file=file_name)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages)


def formatResult(res, files):
    tmp = zip(files, res.values())
    emptyDict = {}

    for file, result in tmp:
        text = result[0]
        tags = ast.literal_eval(result[1])
        file_name = Path(file).stem

        if tags[1] =='nan' or tags[2] == 'nan':
            min_max = inferPeriod(df)
            if min_max:
                tags[1],tags[2] = str(min_max[0]),str(min_max[1])

        emptyDict[file_name] = {"description": text, "tags": tags[0], "min_year": tags[1], "max_year": tags[2]}

    return emptyDict

def generate():
    load_dotenv()

    llm = AzureChatOpenAI(
        azure_endpoint=os.environ["GPT_EndPoint"],
        openai_api_version=os.environ["GPT_APIversion"],
        model=os.environ["GPT_model_name"],
        deployment_name=os.environ["GPT_deployment"],
        temperature=0
    )

    files = glob.glob("financial/*.csv")
    # files = ["../data/data_products/EDGAR_2024_GHG/GHG_by_sector_and_country_CO2_Buildings.csv"]

    res = {}

    for file in files:
        print(file)
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
        file_name = Path(file).stem
        titles, values = fromatInput(df, 2)

        text = generateText(titles, values, Path(file).stem).content
        tags = generateTags(titles, values, Path(file).stem, 7).content

        res[file_name] = [text, tags]

    res = formatResult(res, files)

    # TODO check if files exists and tags exist
    json.dump(res, open("financial/metadata_automatic.json", 'w'))

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


def _generateFunctionFull(name,func):
    llm = models.get_LLM()

    sys_prompt = """ Your task is to summarize a function in 1 to 2 sentences, Focus on what the goal of the function is. Tailor the description to each function
    You are given a short textual description and its paramters.
     Alwyas start the description with the function name  """

    input_prompt = """I would like a description for this function : {func} 
    It is called {name}"""
    input_prompt = input_prompt.format(func=func, name=name)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages)


def generate_function_descriptionsFull():
    res = []

    with open("Catalogs/function_catalog.yml") as stream:
        catalog = yaml.safe_load(stream)

    for func in catalog:
        res_dict = {func: _generateFunctionFull(func, catalog[func]).content}
        res.append(res_dict)

    json.dump(res, open("metadata_function_full.json", 'w'))

def generate_function_descriptionsCode():
    res =[]

    with open("Catalogs/function_catalog.yml") as stream:
        catalog = yaml.safe_load(stream)

    for k,v in code_dict.items():

        res_dict = {k:_generateFunctionFullCode(k,catalog[k],v).content}
        res.append(res_dict)

    json.dump(res, open("metadata_function_full_code.json", 'w'))

def generate_function_descriptionsNoManual():
    res =[]

    with open("Catalogs/function_catalog.yml") as stream:
        catalog = yaml.safe_load(stream)

    for func in catalog:
        catalog[func].pop('description', None)


    for k,v in code_dict.items():

        res_dict = {k:_generateFunctionFullCode(k,catalog[k],v).content}
        res.append(res_dict)

    json.dump(res, open("metadata_function_NoManual2_new.json", 'w'))

def _create_column_info(examples):
    llm = models.get_structured_LLM()

    sys_prompt = """ Your task is to summarize the values of data that you see for that you will be provided with a sample of the data.
    The output should be a valid json with
     "type": description of the what kind of data it is, numerical, texts, dates etc,
     "format": example of how the data is structured, only provide a structure such as YYYY-MM-DD or integers"""

    input_prompt = """I would like a description for this data: 
        {data}
        """
    input_prompt = input_prompt.format(data=examples)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages)

def _create_column_description(format, description):
    llm = models.get_structured_LLM()

    sys_prompt = """ Your task is to provide a short description, as short as possible
    Include data format but do not include examples
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
        columns = []

        if product_name is None or product_name == k:
            des = pd.read_csv(f"{collection_name}/database_description/{k}.csv")
            formats = collection[k]['columns']

            with open(f"{collection_name}/database_description/{k}.csv", 'r') as file:
                i = 0
                for line in file:
                    if i != 0:
                        print(line)


                    i+= 1




            columns.append(f"{col}, type :{res['type']}, format: {res['format']}")


        #    collection[k]["columns"] = columns
        #with open(f"Catalogs/{collection_name}.yml", 'w') as yaml_file:
        #    yaml.dump(collection, yaml_file, default_flow_style=False)

def create_column_info(collection_name, product_name ):
    with open(f"Catalogs/{collection_name}.yml") as stream:
        collection = yaml.safe_load(stream)

    for k in collection.keys():
        columns = []

        if product_name is None or product_name == k:
            data = pd.read_csv(f"{collection_name}/{k}.csv")

            cols = data.columns.tolist()
            for col in cols:
                examples = data[col].unique().tolist()

                if len(examples) < 15:
                    l = len(examples)
                else: l = 15

                examples = random.sample(examples, l)
                print(examples)
                res = _create_column_info(examples)

                columns.append({col:f"type :{res['type']}, format: {res['format']}"})
            print(columns)
                #time.sleep(60)

            collection[k]["columns"] = columns
        with open(f"Catalogs/{collection_name}.yml", 'w') as yaml_file:
            yaml.dump(collection, yaml_file, default_flow_style=False)

def fix_error(collection_name):
    with open(f"Catalogs/{collection_name}.yml") as stream:
        collection = yaml.safe_load(stream)

    for k in collection.keys():
        if k != "votes":
            columns = collection[k]['columns']
            new =[]
            for col in columns:
                parts = col.split(",",maxsplit=1)
                new.append({parts[0]:parts[1].strip()})

            collection[k]["columns"] = new

        with open(f"Catalogs/{collection_name}.yml", 'w') as yaml_file:
                yaml.dump(collection, yaml_file, default_flow_style=False)


def format_descriptions(collection_name, product_name ):
    with open(f"Catalogs/{collection_name}.yml") as stream:
        collection = yaml.safe_load(stream)

if __name__ == "__main__":
    #generateCollection("toxicology")
    #generate_function_descriptions()
    #generate_function_descriptionsCode()
    #generate_function_descriptionsNoManual()
    #create_column_info("codebase_community","votes")
    #print(create_column_description("european_football_2",None))
    fix_error("toxicology")