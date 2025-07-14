import pandas as pd
import re
import requests
import json
import io
import numpy as np
from uuid import uuid4
import langchain_openai
import yaml
import ast

from langchain_core.runnables import RunnableLambda
from langchain_core.prompts import PromptTemplate
from Backend import models
from dotenv import load_dotenv
load_dotenv()
def getCatalog(file):
    if "/" in file:
        file = file.split("/")[-1]

    try:
        with open("Backend/data/Catalogs/catalog.yml") as stream:
            catalog = yaml.safe_load(stream)
    except FileNotFoundError:
        return "could not find the main catalog"
    for collection in catalog:
        collection =  catalog[collection]
        if file in collection['products']:
            try:
                with open("Backend/data/Catalogs/" + collection['name'] + ".yml") as stream:
                    collection_dict = yaml.safe_load(stream)
                    print(file)
                    return collection_dict[file]
            except FileNotFoundError:
                return "could not find the specific collection catalog"
            except KeyError:
                return collection_dict

def rephrase_query_prod(step,org):
    sys_prompt = """Your goal is to rephrase a short description of a processing step into a longer more complete query which can be used to query a vector store
    The goal is to identify a data product, use the context provided by the original query to extend the provided step
    Only provide the reformulated query"""

    input_prompt= PromptTemplate.from_template(""" The task I am currently trying to achieve is:{step}, the original query is: {org}""")
    input_prompt = input_prompt.format( step=step, org=org)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    llm = models.get_LLM()
    return llm.invoke(messages).content

org = "What is Copycat's race?"
step = "retrieve race dataset"

#print(rephrase_query_prod(step, org))

a =set([(1,2),(2,3)])
b = set([(1,2),(5,4)])
print(a&b)
print(a-b)
print(b-a)

def _get_df(product, collection):
    response = requests.get(f"http://127.0.0.1:5000/products/{collection}/{product}")
    content = json.loads(response.text)
    df = pd.read_json(io.StringIO(content['data']))
    return df


def q_892():
    df1 = _get_df("drivers", "formula_1")
    df2 = _get_df("results", "formula_1")
    df2 = df2.groupby(by=["driverId"])
    df2 = df2["points"].sum()
    df2 = df2.nlargest(n=1)

    res = df1.merge(df2, suffixes=["", "_y"], left_on="driverId", right_on='driverId')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)
    print(res.columns)
    res = res.drop(labels=["points"],axis=1)
    res.to_csv("sql_result_892.csv",index=False)
    """
    {"plans": [{"function": "http://127.0.0.1:5200/retrieve","filter_dict": {"product": "http://127.0.0.1:5000/products/formula_1/drivers"}},
               {"function": "http://127.0.0.1:5200/retrieve","filter_dict": {"product": "http://127.0.0.1:5000/products/formula_1/results"}},
               {"function": "http://127.0.0.1:5200/sum", "filter_dict": {"group_by": "driverId", "column": "points"}},
               {"function": "http://127.0.0.1:5200/max", "filter_dict": {"columns": "driverId", "rows": 1}},
               {"function":'combination', 'filter_dict': {"columns_left": "driverId", "columns_right": "driverId", "type": "equals", "values": ["None"]}}]}
    """
q_892()
