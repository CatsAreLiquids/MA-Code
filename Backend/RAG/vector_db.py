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

def _init_bm25(config,collection):
    if config is not None:
        docs = get_docs("",10000,config['filter'],collection)
    else:
        docs = get_docs("", 10000,collection=collection)
    return BM25Retriever.from_documents(docs)

def _add_Function(functionName: str, description: dict):
    vector_store = models.getVectorStore()

    text = f"function name:{functionName}\n"
    for k, v in description.items():
        text += f"{k}: {v}\n"

    doc = [Document(page_content=text, metadata={"type": "function_text", "id": str(uuid.uuid4())})]
    vector_store.add_documents(doc)


def add_Functions(functionName: str | None):
    try:
        with open("../data/Catalogs/function_catalog_text_only.yml") as stream:
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


def _add_doc (productName: str, description: dict):
    vector_store = models.getVectorStore()

    tags = description["tags"]
    meta_dict = {"tags": tags,
                 "type": "product",
                 "id": str(uuid.uuid4())}

    doc = Document(page_content=description["description"], metadata=meta_dict)
    vector_store.add_documents([doc])


def add_docs(collection, productName: str | None):

    try:
        data = json.load(open(f"../data/{collection}/metadata_automatic.json"))
        if productName is not None:
            print(data[productName])
            _add_doc(productName, data[productName])
            return
    except FileNotFoundError:
        print(f"Could not find the product describtions at /data/{collection}/. Wont proceed")
        return
    except KeyError:
        print("Could not find the specified product in the catalog. Wont proceed")
        return

    for k, v in data.items():
        _add_doc(k, v)


def delete(id: List | None, collection= None):

    vector_store = models.getVectorStore(collection)

    if id is None:
        vector_store.delete_collection()
    else:
        vector_store.delete(id)

def get_docs_score(query,max:int,filter=None):
    vector_store = models.getVectorStore()
    res = vector_store.similarity_search_with_score(query, k=max, filter=filter)

    for i in res:
        print(i)

def get_docs(query,max:int,filter=None,collection=None):

    vector_store = models.getVectorStore(collection)
    return vector_store.similarity_search(query, k=max,filter=filter)


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


def getChain(prompt,config):
    vector_store = models.getVectorStore()
    llm = models.get_LLM()
    if config is not None:
        retriever = vector_store.as_retriever(search_type="mmr",search_kwargs= config)
    else:
        retriever = vector_store.as_retriever()

    bm25_retriever = _init_bm25(config)

    ensemble_retriever = EnsembleRetriever(k=5,
        retrievers=[bm25_retriever, retriever], weights=[0.6, 0.4]
    )

    prompt = PromptTemplate.from_template(prompt)

    def format_docs(docs):
        reorder = LongContextReorder()
        docs = reorder.transform_documents(docs)
        #for i in docs:
        #    print(i)
        return "\n\n".join(doc.page_content for doc in docs)

    rag_chain = (
            {"context": ensemble_retriever | format_docs, "question": RunnablePassthrough()}
            | prompt
            | llm
            | StrOutputParser()
    )

    return rag_chain

def reorder1(docs):
    print(docs)

if __name__ == "__main__":
    db = models.getVectorStore("DB_PEDIA")
    print(db.similarity_search("Apollo astronauts who walked on the Moon"))

    delete(id = None)
    #get_docs_score(query = "",max= 10,filter={"type": {"$eq": "function_text"}})
    #add_Functions("getNRows")
