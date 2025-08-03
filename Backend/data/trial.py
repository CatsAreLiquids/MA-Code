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
import requests
import ast
import json
import pandas as pd
import requests
import io
from numpy.random import default_rng

load_dotenv()


def generateSummary(query: str, prod_descriptions):
    sys_prompt = """ Your task is to summarize the data provided to you. You are provided with the collection name, the data product name and the column name of the data that should be summarized.
            Additioinally you are provided with up to 20 random samples form the data
            Keep the summary short and concise, focus on genreal facts rather than specific data points. It should be not more than thre sentences
        """
    input_prompt = PromptTemplate.from_template("""
                Collection Name:{collection}
                Data product name:{file_name}
                column name:{column}
                random sample: {sample}
                """)
    input_prompt = input_prompt.format(collection=query, file_name=prod_descriptions,column=,sample=)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]

    llm = models.get_LLM()

    return llm.invoke(messages)

def stuff():
    response = requests.get("http://127.0.0.1:5000/catalog/columns", json={"file": file})
    data = json.loads(response.text)

def summarize_column(column,data_product_url):
    response = requests.get(data_product_url, json={"file": file})
    data = json.loads(response.text)
    try:
        df = pd.read_json(io.StringIO(data['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(data['data']))

    values = df[column].unique().tolist()

    if len(values) > 20:
        rng = default_rng()
        idx = rng.choice(len(values), size=20, replace=False)#
    values_tmp = values[idx]



def annominize():
    pass

if __name__ == "__main__":
    column = "Diagnosis"
    data_product_url =  "http://127.0.0.1:5000/products/thrombosis_prediction/Patient"

    summarize_column(column,data_product_url)