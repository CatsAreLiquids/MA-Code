import ast
import itertools
import os

import pandas as pd
from langchain_core.documents import Document
from langchain_postgres import PGVector
from langchain_postgres.vectorstores import PGVector
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from dotenv import load_dotenv
import json
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_core.runnables import RunnableLambda
from langchain_core.prompts import PromptTemplate
from langchain.retrievers import EnsembleRetriever
from langchain_community.retrievers import BM25Retriever
import re
from langchain_community.document_transformers import LongContextReorder
from typing import Optional, List
import uuid

from Backend import models
from dotenv import load_dotenv
import yaml
import numpy as np
import copy

from numpy import dot
from numpy.linalg import norm
from collections import defaultdict
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import numpy as np

import matplotlib
import matplotlib as mpl
matplotlib.use('TkAgg')
load_dotenv()




def _add_Function(functionName: str, description: dict):
    vector_store = models.getVectorStore()

    text = f"function name:{functionName}\n"
    for k, v in description.items():
        text += f"{k}: {v}\n"

    doc = [Document(page_content=text, metadata={"type": "function_NoManual", "id": str(uuid.uuid4()),"name":functionName})]
    vector_store.add_documents(doc)


def add_Functions(functionName: str | None):
    try:
        with open("../data/Catalogs/function_catalog.yml") as stream:
            data = yaml.safe_load(stream)
            if functionName is not None:
                _add_Function(functionName, data[functionName])
                return
    except FileNotFoundError:
        print("Could not find the file catalog at data/Catalogs/ . Wont proceed")
        return
    except KeyError:
        print("Could not find the specified function in the catalog. Wont proceed")
        return

    for k, v in data.items():
        _add_Function(k, v)

def _add_FunctionTest(data):
    vector_store = models.getVectorStore()

    text = f"function name:"
    for k, v in data.items():
        functionName = k
        text += f" {k}:\n {v}\n"

    doc = [Document(page_content=text, metadata={"type": "function_name", "id": str(uuid.uuid4()),"name":functionName})]
    vector_store.add_documents(doc)

def add_Functions_Test(functionName: str | None):
    datas = json.load(open(f"../data/metadata_function_name.json"))

    for data in datas:
            _add_FunctionTest(data)


def _add_doc(productName: str, description: dict,collection):
    vector_store = models.getVectorStore()

    tags = description["tags"]
    meta_dict = {"tags": tags,
                 "type": "product",
                 "collection":collection,
                 "id": str(uuid.uuid4())}

    doc = Document(page_content=description["description"], metadata=meta_dict)
    vector_store.add_documents([doc])


def add_docs(collection, productName: str | None):
    try:
        data = json.load(open(f"../data/{collection}/metadata_automatic.json"))
        if productName is not None:
            print(data[productName])
            _add_doc(productName, data[productName],collection)
            return
    except FileNotFoundError:
        print(f"Could not find the product describtions at /data/{collection}/. Wont proceed")
        return
    except KeyError:
        print("Could not find the specified product in the catalog. Wont proceed")
        return

    for k, v in data.items():
        _add_doc(k, v,collection)


def _add_collection(collection_name, description):
    vector_store = models.getVectorStore("collection_level")

    meta_dict = {"name": collection_name,
                 "id": str(uuid.uuid4())}

    doc = Document(page_content=description, metadata=meta_dict)
    vector_store.add_documents([doc])


def add_collections(collection_name=None):
    try:
        with open("../data/Catalogs/catalog.yml") as stream:
            catalog = yaml.safe_load(stream)
            if collection_name is not None:
                _add_collection(catalog[collection_name]["name"], catalog[collection_name]["description"])
    except FileNotFoundError:
        print("could not find the main catalog")
        return
    except KeyError:
        print("Could not find the collection. Wont proceed")
        return

    for k, v in catalog.items():
        _add_collection(catalog[k]["name"], catalog[k]["description"])


def delete(id: List | None, collection=None):
    vector_store = models.getVectorStore(collection)

    if id is None:
        vector_store.delete_collection()
    else:
        vector_store.delete(id)


def get_docs_score(query, max: int, filter=None):
    vector_store = models.getVectorStore()
    res = vector_store.similarity_search_with_score(query, k=max, filter=filter)

    for i in res:
        print(i)


def get_docs(query, max: int, filter=None, collection=None):
    vector_store = models.getVectorStore(collection)
    #res = vector_store.similarity_search_with_score(query, k=max, filter=filter)
    #for i in res:
    #    print(i)

    return vector_store.similarity_search(query, k=max, filter=filter)


def evalEmbbeds(docs):

    emb_func = models.get_embeddings()

    docs = get_docs(query = "",max= 30,filter={"type": {"$eq": "function_NoManual"}})
    df_dict = defaultdict(list)
    for doc in docs:
        df_dict[doc.metadata["name"]]

    for doc in docs:
        self_emb = emb_func.embed_query(doc.page_content)
        for doc2 in docs:
            other_emb = emb_func.embed_query(doc2.page_content)
            cos_sim = dot(self_emb, other_emb) / (norm(self_emb) * norm(other_emb))
            df_dict[doc.metadata["name"]].append(cos_sim)

    names = [doc.metadata["name"] for doc in docs]
    df = pd.DataFrame(df_dict)
    df["function"] = names
    df = df.set_index('function')
    df.to_csv("functions_cos_sim_NoManual.csv")

def stuff():
    df = pd.read_csv("functions_cos_sim_NoManual.csv")
    ax = plt.axes()

    heatmap= sns.heatmap(df.loc[:, df.columns != "function"],yticklabels=df["function"].tolist(),cmap=sns.cubehelix_palette(as_cmap=True),vmin=0)
    fig = heatmap.get_figure()
    fig.tight_layout()
    fig.savefig("NoManual.png")

if __name__ == "__main__":
    # add_collections()
    db = models.getVectorStore()
    #add_Functions_Test(None)
    #print(rephrase_query("retrieve drivers dataset","State code numbers of top 3 yougest drivers. How many Netherlandic drivers among them?"))
    #add_collections()


    prompt = """Your task is to help find the best fitting data product. You are provided with a user query .
                    Provide the most likely fitting data products, always provide the data products name and why it would fit this query
            Question: {question} 
            Context: {context} 
            Answer:
        """
    #multiLevelChain("What is the ratio of customers who pay in EUR against customers who pay in CZK?", prompt,
    #                {"filter": {"type": {"$eq": "product"}}})

    #add_Functions("sortby")
    #delete(id= ["93e6e754-d8d9-4e1b-9bd7-24da841c0644","c4a436be-0190-4d80-867e-ec18f0e821c6"],collection=None)


    get_docs_score(query = "",max= 100,filter={"type": {"$eq": 'function_NoManual'}})#,filter={"type": {"$eq": "product"}}#,filter={"type": {"$eq": "function_NoManual"}}
    #add_Functions("retrieve")
    #add_docs("toxicology",None)
    #evalEmbbeds("")
    #stuff()
