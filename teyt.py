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

def manual_correction(agent_result):
    print(agent_result)
    agent_result = ast.literal_eval(agent_result)
    plan = agent_result["plans"]

    def helper(instance):
        if isinstance(instance, (int, float, complex, str)) and not isinstance(instance, bool):
            return instance
        elif isinstance(instance,list):
            for i in range(len(instance)):
                if not isinstance(instance[i], (int, float, complex, str)):
                    instance[i] = str(instance[i])
            return instance
        else:
            return str(instance)

    for i in range(len(plan)):
        params = plan[i]['filter_dict']
        for k, v in params.items():
            print(k,v)
            if isinstance(v, dict):
                for kk, vv in v.items():
                    if isinstance(vv, dict):
                        for kkk, vvv in vv.items():
                            params[k][kk][kkk] = helper(vvv)

                    else:
                        params[k][kk]= helper(vv)
            else:
                params[k] = helper(v)

    agent_result["plans"] = plan

    return agent_result


a = [1,2]
b = [2,1]
print(a==b)

l = {'function':'http://127.0.0.1:5200/sum','filter_dict':{'column': 'remaining budget',} }
{'function':'http://127.0.0.1:5200/sum','filter_dict':{'column': ['remaining budget'], 'group_by': ['event'],} }
{'function':'http://127.0.0.1:5200/filter','filter_dict':{ 'condition': {'remaining_budget': {'min': 50, 'max': 100}},} }

df = pd.read_csv('Backend/data/card_games/cards.csv')
print(df.shape)