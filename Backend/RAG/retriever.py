import ast
import itertools
import os
import requests
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
from Backend.RAG.vector_db import get_docs
import matplotlib
import matplotlib as mpl
matplotlib.use('TkAgg')
load_dotenv()


def _init_bm25(config, collection):
    if config is not None:
        docs = get_docs("", 10000, config['filter'], collection)
    else:
        docs = get_docs("", 10000, collection=collection)

    return BM25Retriever.from_documents(docs)

def multiLevelChain(query, prompt, config):
    db_outer = models.getVectorStore("collection_level")
    llm = models.get_structured_LLM()

    cot = True
    if cot:
        collection_prompt = """Your task is to find the data collection that can answer a users query, in a valid json format return the collections name and why and how it can answer the query
            your response should look like this: {{'collection_name': 'name','reason':'reason'}}
            Context: {context} 
            Answer:
        """
    else:
        collection_prompt = """Your task is to find the data collection that can answer a users query, in a valid json format return the collections name
                your response should look like this: {{'collection_name': 'name','reason':'reason'}}
                    Context: {context} 
                    Answer:
            """

    retriever = db_outer.as_retriever()
    docs = retriever.invoke(query)
    fdocs = [doc.page_content for doc in docs]
    fdocs = "\n\n".join(doc for doc in fdocs)
    collection_prompt = PromptTemplate.from_template(collection_prompt).format(context=fdocs)
    messages = [
        ("system", collection_prompt),
        ("human", query),
    ]
    response = llm.invoke(messages)
    collection = response["collection_name"]

    if config is None:
        config = {"collection_name": {"$eq": collection}}
        vec = models.getVectorStore()
        standard_retriever = vec.as_retriever(search_type="mmr")
    else:
        vec = models.getVectorStore()
        standard_retriever = vec.as_retriever(search_type="mmr", search_kwargs=config)
        config["filter"]["collection_name"] = {"$eq": collection}

    collection_only_retriever = models.getVectorStore().as_retriever(search_type="mmr", search_kwargs=config)

    ensemble_retriever = EnsembleRetriever(k=5,
                                           retrievers=[standard_retriever, collection_only_retriever],
                                           weights=[0.4, 0.6])
    def format_docs(docs):
        reorder = LongContextReorder()
        docs = reorder.transform_documents(docs)
        return "\n\n".join(doc.page_content for doc in docs)

    prompt = PromptTemplate.from_template(prompt)

    rag_chain = (
            {"context": ensemble_retriever | format_docs, "question": RunnablePassthrough()}
            | prompt
            | llm
            | StrOutputParser()
    )
    return rag_chain


def getChain(prompt, config,collection=None):
    vector_store = models.getVectorStore(collection)
    llm = models.get_LLM()
    #llm = llm.with_structured_output(method="json_mode")
    if config is not None:
        retriever = vector_store.as_retriever( search_kwargs=config)
    else:
        retriever = vector_store.as_retriever()

    bm25_retriever = _init_bm25(config,None)

    ensemble_retriever = EnsembleRetriever(k=5,
                                           retrievers=[bm25_retriever, retriever], weights=[0.5, 0.5]
                                           )

    prompt = PromptTemplate.from_template(prompt)

    def format_docs(docs):
        reorder = LongContextReorder()
        docs = reorder.transform_documents(docs)
        return "\n\n".join(doc.page_content for doc in docs)

    rag_chain = (
            {"context": ensemble_retriever | format_docs, "question": RunnablePassthrough()}
            | prompt
            | llm
            | StrOutputParser()
    )

    return rag_chain


def getMultiLevelEvaluation( prompt, config,query):
    db_outer = models.getVectorStore("collection_level")
    llm = models.get_structured_LLM()

    cot = True
    if cot:
        collection_prompt = """Your task is to find the data collection that can answer a users query, in a valid json format return the collections name and why and how it can answer the query
            your response should be a valid json with : 'collection_name': the name of the data collection,'reason': where reason should be a short explanation as to why it is the ocrrect dataset 
            Context: {context} 
            Answer:
        """
    else:
        collection_prompt = """Your task is to find the data collection that can answer a users query, in a valid json format return the collections name
                your response should look like this: 'collection_name': of the data set,'reason':where reason should be a short explanation as to why it is the ocrrect dataset
                    Context: {context} 
                    Answer:
            """

    retriever = db_outer.as_retriever()
    docs = retriever.invoke(query)
    fdocs = [doc.page_content for doc in docs]
    fdocs = "\n\n".join(doc for doc in fdocs)

    collection_prompt = PromptTemplate.from_template(collection_prompt).format(context=fdocs)
    messages = [
        ("system", collection_prompt),
        ("human", query),
    ]
    response = llm.invoke(messages)
    collection = response["collection_name"]

    if config is None:
        config = {"collection_name": {"$eq": collection}}
        vec = models.getVectorStore()
        standard_retriever = vec.as_retriever()
    else:
        vec = models.getVectorStore()
        standard_retriever = vec.as_retriever(config={"filter": {"type": {"$eq": "product"}}})
        config = {"filter": {"collection_name": {"$eq": collection}}}
    collection_only_retriever = models.getVectorStore().as_retriever( config=config)

    ensemble_retriever = EnsembleRetriever(k=5,
                                           retrievers=[standard_retriever, collection_only_retriever],
                                           weights=[0.7, 0.3])
    reorder = LongContextReorder()
    docs = ensemble_retriever.invoke(query)
    docs = [doc.page_content for doc in docs]

    fdocs = reorder.transform_documents(docs)
    fdocs = "\n\n".join(doc for doc in fdocs)


    sys_prompt = PromptTemplate.from_template(prompt).format(context=fdocs)
    messages = [
        ("system", sys_prompt),
        ("human", query),
    ]
    llm = models.get_structured_LLM()
    return {"response": llm.invoke(messages)["name"], "docs": docs}


def getEvaluationChain(sys_prompt, config, query,collection=None):
    vector_store = models.getVectorStore(collection)

    if config is not None:
        retriever = vector_store.as_retriever(search_kwargs=config,k=5)
    else:
        retriever = vector_store.as_retriever()

    bm25_retriever = _init_bm25(config, collection)

    ensemble_retriever = EnsembleRetriever(k=5,
                                           retrievers=[bm25_retriever, retriever], weights=[0.3, 0.7]
                                           )

    reorder = LongContextReorder()

    docs = ensemble_retriever.invoke(query)
    docs = retriever.invoke(query)
    docs = [doc.page_content for doc in docs]
    #print(len(docs))
    fdocs = reorder.transform_documents(docs)
    fdocs = "\n\n".join(doc for doc in docs)

    #query = rephrase_query_prod(query,org)
    sys_prompt = PromptTemplate.from_template(sys_prompt).format(context=fdocs)
    messages = [
        ("system", sys_prompt),
        ("human", query),
    ]

    llm = models.get_structured_LLM()

    return {"response": llm.invoke(messages)["name"], "docs": docs}

def product_rag(step):
    sys_prompt = """Your task is to help find the best fitting data product. You are provided with a user query.
                Provide the most likely fitting data product, always provide the data products name and why it would fit this query.
                Only provide one data product
        Context: {context} 
        The output should be a valid json with 'name': as the data product name and 'reason':
    """
    vector_store = models.getVectorStore()
    llm = models.get_structured_LLM()

    config = {"filter": {"type": {"$eq": "product"}}}
    retriever = vector_store.as_retriever(search_kwargs=config,k=5)

    bm25_retriever = _init_bm25(config,None)
    ensemble_retriever = EnsembleRetriever(k=5,
                                           retrievers=[bm25_retriever, retriever], weights=[0.5, 0.5]
                                           )


    docs = ensemble_retriever.invoke(step)
    docs = [doc.page_content for doc in docs]

    sys_prompt = PromptTemplate.from_template(sys_prompt).format(context=docs)
    messages = [
        ("system", sys_prompt),
        ("human", step),
    ]

    return llm.invoke(messages)




def collection_rag( query,config):

    collection = "collection_level"
    sys_prompt = """Your task is to find the data collection that can answer a users query, in a valid json format return the collections name
                    You are provided with a list of data collection descriptions. Return the name of the collection not of a fitting product
                    your response should look like this: 'collection_name': 'name','reason':'reason'
                Context: {context} 
                Answer:
            """

    vector_store = models.getVectorStore(collection)
    llm = models.get_structured_LLM()

    if config is not None:
        retriever = vector_store.as_retriever(search_kwargs=config,k=5)
    else:
        retriever = vector_store.as_retriever()

    bm25_retriever = _init_bm25(config, collection)
    ensemble_retriever = EnsembleRetriever(k=5,
                                           retrievers=[bm25_retriever, retriever], weights=[0.5, 0.5]
                                           )


    docs = ensemble_retriever.invoke(query)
    docs = retriever.invoke(query)
    docs = [doc.page_content for doc in docs]


    sys_prompt = PromptTemplate.from_template(sys_prompt).format(context=docs)
    messages = [
        ("system", sys_prompt),
        ("human", query),
    ]

    return  llm.invoke(messages)

def function_rag(step):
    sys_prompt = """Your task is to find a the most fitting function that could solve problem described in the provided step.
                    As a valid json format return the function name your response should look like this: 'function_name': 'name','reason':'reason'
                    If you can not find a fitting function return 'function_name': None

                    Context: {context} 
                    Answer:
        """

    pattern = r"function name: *([a-z,A-Z]*):(?:\\n [a-z,A-Z]*\\n|\n [a-z,A-Z]*\n)"
    vector_store = models.getVectorStore()

    retriever = vector_store.as_retriever(search_kwargs={"filter": {"type": {"$eq": "function_NoManual2"}}},k=5)

    bm25_retriever = _init_bm25({"filter": {"type": {"$eq": "function_name"}}},None)

    ensemble_retriever = EnsembleRetriever(k=5,
                                           retrievers=[bm25_retriever, retriever], weights=[0.5, 0.5]
                                           )
    def helper(doc):
        name = re.findall(pattern,doc)

        try:
            response = requests.get("http://127.0.0.1:5200/catalog", json={"function_name": name[-1]})
            data = json.loads(response.text)
            return data["description"]
        except KeyError:
            return doc

    docs = ensemble_retriever.invoke(step)

    docs = [doc.page_content for doc in docs]
    docs = [ helper(doc) if len(re.findall(pattern,doc))> 0 else doc for doc in docs ]
    fdocs = "\n\n".join(doc for doc in docs)

    sys_prompt = PromptTemplate.from_template(sys_prompt).format(context=fdocs)
    messages = [
        ("system", sys_prompt),
        ("human", step),
    ]

    llm = models.get_structured_LLM()

    return llm.invoke(messages)


def getEvaluationChainFunc( config, query, collection=None):
    sys_prompt = """Your task is to find a the most fitting function that could solve problem described in the provided step.
                    As a valid json format return the function name your response should look like this: 'function_name': 'name','reason':'reason'

                    Context: {context} 
                    Answer:
        """

    pattern = r"function name: *([a-z,A-Z]*):(?:\\n [a-z,A-Z]*\\n|\n [a-z,A-Z]*\n)"
    vector_store = models.getVectorStore(collection)

    if config is not None:
        retriever = vector_store.as_retriever(search_kwargs=config,k=5)
    else:
        retriever = vector_store.as_retriever(k=5)


    config = {"filter": {"type": {"$eq": "function_name"}}}
    bm25_retriever = _init_bm25(config, collection)

    ensemble_retriever = EnsembleRetriever(k=5,
                                           retrievers=[bm25_retriever, retriever], weights=[0.3, 0.7]
                                           )
    def helper(doc):
        name = re.findall(pattern,doc)

        try:
            response = requests.get("http://127.0.0.1:5200/catalog", json={"function_name": name[-1]})
            data = json.loads(response.text)
            return data["description"]
        except KeyError:
            return doc

    docs = ensemble_retriever.invoke(query)



    #docs = retriever.invoke(query)
    docs = [doc.page_content for doc in docs]
    docs = [ helper(doc) if len(re.findall(pattern,doc))> 0 else doc for doc in docs ]
    reorder = LongContextReorder()
    fdocs = reorder.transform_documents(docs)
    fdocs = "\n\n".join(doc for doc in fdocs)

    #query = rephrase_query(query)
    sys_prompt = PromptTemplate.from_template(sys_prompt).format(context=fdocs)
    messages = [
        ("system", sys_prompt),
        ("human", query),
    ]

    llm = models.get_structured_LLM()

    return {"response": llm.invoke(messages)["function_name"], "docs": docs}

def rephrase_query_func(step):
    sys_prompt = """Your goal is to rephrase a short description of a processing step into a longer more complete query which can be used to query a vector store
    The goal is to identify the functions necerssary, therfore focus on the processing step. Keep the rephrasing short and focus on the action not the parameters"""

    input_prompt= PromptTemplate.from_template(""" The task I am currently trying to achieve is:{step}""")
    input_prompt = input_prompt.format( step=step)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    llm = models.get_LLM()
    return llm.invoke(messages).content

def rephrase_query_prod(step,org):
    sys_prompt = """Your goal is to rephrase a short description of a processing step into a longer more complete query which can be used to query a vector store
    The goal is to identify a data product, use the context provided by the original query to extend the provided step"""

    input_prompt= PromptTemplate.from_template(""" The task I am currently trying to achieve is:{step}, the original query is: {org}""")
    input_prompt = input_prompt.format( step=step, org=org)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    llm = models.get_LLM()
    return llm.invoke(messages).content