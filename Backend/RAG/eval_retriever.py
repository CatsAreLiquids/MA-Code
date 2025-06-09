import os

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

load_dotenv()

def getEvaluationChain(sys_prompt,config,input,collection=None):
    vector_store = models.getVectorStore(collection)
    llm = models.get_LLM()
    if config is not None:
        retriever = vector_store.as_retriever(search_type="mmr",search_kwargs= config)
    else:
        retriever = vector_store.as_retriever()

    bm25_retriever = _init_bm25(config,collection)

    ensemble_retriever = EnsembleRetriever(k=5,
        retrievers=[bm25_retriever, retriever], weights=[0.6, 0.4]
    )

    reorder = LongContextReorder()

    docs = ensemble_retriever.invoke(input)
    docs = [doc.page_content for doc in docs]
    fdocs = reorder.transform_documents(docs)
    fdocs = "\n\n".join(doc for doc in fdocs)


    sys_prompt = PromptTemplate.from_template(sys_prompt).format(context=fdocs)
    messages = [
        ("system",sys_prompt),
        ("human", input),
    ]

    llm = models.get_LLM()

    return {"response":llm.invoke(messages).content,"docs":docs}

def productRetriever_eval(query):
    sys_prompt = """Your task is to help find the best fitting data product. You are provided with a user query .
                Provide the most likely fitting data products, always provide the data products name and why it would fit this query
        Context: {context} 
        Answer:
    """
    mod_query = f"I am looking for data products that can answer the query: '{query}'"
    config = {"filter": {"type": {"$eq": "product"}}}

    return getEvaluationChain(sys_prompt, config,mod_query)


def functionRetriever_eval(query,type):
    """
    Searches and returns Data files aboout the functions accesible for the processing. Only usefull when trying to identify a function
    Args:
        step: a specific step in an execution plan, describing some kind of agreggation,transformation or filtering
    Returns:
        a text descrbing potential functions
    """
    sys_prompt = """Your task is to find a the top 2 fitting functions that could solve problem described in the provided step.
                Provide the most fitting function and its full description. Your answer is mewant to be short and precise exclude any additional examples.

                Context: {context} 
                Answer:
    """
    mod_query = f"I am looking for a function that can solve the following problem for me :{query}"
    config = {"filter": {"type": {"$eq": type}}}
    return getEvaluationChain(sys_prompt, config,mod_query)

def db_pediaRetriever_eval(query):
    """
    Searches and returns Data files aboout the functions accesible for the processing. Only usefull when trying to identify a function
    Args:
        step: a specific step in an execution plan, describing some kind of agreggation,transformation or filtering
    Returns:
        a text descrbing potential functions
    """
    sys_prompt = """Your task is to help find the best fitting data entry. You are provided with a user query .
                Provide the most likely fitting data entry, always provide the data entrys name and why it would fit this query
        Context: {context} 
        Answer:
    """
    mod_query = f"I am looking for a data entry that can solve the following problem for me :{query}"
    config = None
    return getEvaluationChain(sys_prompt, config,mod_query,collection="DB_PEDIA")

def multilevelRetriever(query,type,collection):
    sys_prompt = """Your task is to help find the best fitting data product. You are provided with a user query .
                    Provide the most likely fitting data products, always provide the data products name and why it would fit this query
            Context: {context} 
            Answer:
        """

    pass
