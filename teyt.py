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

def helper(x):
    i = 0

    return len(x)

file = "Backend/evaluation/bird_mini_dev/prototype_eval.csv"
df = pd.read_csv(file)
print(df.shape)
print(df.dropna().shape)

test = {"conditions": {"CharterNum": {"not_null": true}}}