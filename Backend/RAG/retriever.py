import json
import re

import matplotlib
import requests
from dotenv import load_dotenv
from langchain.retrievers import EnsembleRetriever
from langchain_community.retrievers import BM25Retriever
from langchain_core.prompts import PromptTemplate

from Backend import models
from Backend.RAG.vector_db import get_docs

load_dotenv()

def _init_bm25(config, collection):
    if config is not None:
        docs = get_docs("", 10000, config['filter'], collection)
    else:
        docs = get_docs("", 10000, collection=collection)

    return BM25Retriever.from_documents(docs)

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


