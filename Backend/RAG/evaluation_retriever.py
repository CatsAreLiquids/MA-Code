import re
import requests
import json
from dotenv import load_dotenv
from langchain_core.prompts import PromptTemplate
from langchain.retrievers import EnsembleRetriever
from langchain_community.document_transformers import LongContextReorder

from Backend import models
from Backend.RAG.retriever import _init_bm25

load_dotenv()

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


    config = {"filter": {"type": {"$eq": "function_NoManual2"}}}
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
    fdocs = "\n\n".join(doc for doc in docs)

    sys_prompt = PromptTemplate.from_template(sys_prompt).format(context=fdocs)
    messages = [
        ("system", sys_prompt),
        ("human", query),
    ]
    llm = models.get_structured_LLM()

    return {"response": llm.invoke(messages)["functions"], "docs": docs}


def getMultiLevelEvaluation( prompt, config,query):
    db_outer = models.getVectorStore("collection_level")
    llm = models.get_structured_LLM()

    cot = True
    if cot:
        collection_prompt = """Your task is to find the data collection that can answer a users query, in a valid json format return the collection_name and why and how it can answer the query
            your response should be a valid json with the paramerts 'collection_name' and reason. 'collection_name': as the name of the data collection,'reason': where reason should be a short explanation as to why it is the ocrrect dataset 
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

    if "collection_name" in response:
        collection = response["collection_name"]
    else:
        collection = response["name"]

    if config is None:
        config = {"collection_name": {"$eq": collection}}
        vec = models.getVectorStore()
        standard_retriever = vec.as_retriever()
    else:
        vec = models.getVectorStore()
        standard_retriever = vec.as_retriever(search_kwargs={"filter": {"type": {"$eq": "name_2"}}})
        config = {"filter": {"collection_name": {"$eq": collection}}}

    collection_only_retriever = models.getVectorStore().as_retriever(search_kwargs=config)

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
                                           retrievers=[bm25_retriever, retriever], weights=[0.7, 0.3]
                                           )
    reorder = LongContextReorder()
    docs = ensemble_retriever.invoke(query)
    docs = [doc.page_content for doc in docs]
    fdocs = reorder.transform_documents(docs)
    fdocs = "\n\n".join(doc for doc in fdocs)

    sys_prompt = PromptTemplate.from_template(sys_prompt).format(context=fdocs)
    messages = [
        ("system", sys_prompt),
        ("human", query),
    ]

    llm = models.get_structured_LLM()

    return {"response": llm.invoke(messages)["name"], "docs": docs}

def productRetriever_eval(query):
    sys_prompt = """Your task is to help find the best fitting data product. You are provided with a user query.
                Provide the most likely fitting data product, always provide the data products name and why it would fit this query.
                Only provide one data product
        Context: {context} 
        The output should be a valid json with 'name': as the data product name and 'reason':
    """
    mod_query = f"I am looking for data products that can answer the query: '{query}'"
    config = {"filter": {"type": {"$eq": "product"}}}

    return getEvaluationChain(sys_prompt, config,mod_query)

def productRetriever_eval_both(query,step):
    sys_prompt = """Your task is to help find the best fitting data product. You are provided with a user query.
                Provide the most likely fitting data product, always provide the data products name and why it would fit this query.
                Only provide one data product
        Context: {context} 
        The output should be a valid json with 'name': as the data product name and 'reason':
    """
    mod_query = f"I am looking for data products that can answer the query: '{query}', specifically the step:{step}"
    config = {"filter": {"type": {"$eq": "product"}}}

    return getEvaluationChain(sys_prompt, config,mod_query)


def functionRetriever_eval(query,type):

    config = {"filter": {"type": {"$eq": "function_NoManual2"}}}
    return getEvaluationChainFunc(config,query)


def functionRetriever_hybrid(query,type):

    mod_query = f"I am looking for a function that can solve the following problem for me :{query}"
    config = {"filter": {"type": {"$eq": "function_NoManual2"}}}
    return getEvaluationChainFunc( config,mod_query)

def multilevelRetriever(query):
    sys_prompt = """Your task is to help find the best fitting data product. You are provided with a user query.
                Provide the most likely fitting data product, always provide the data products name and why it would fit this query.
                Only provide one data product
        Context: {context} 
        The output should be a valid json with 'name': as the data product name and 'reason': where reason should be a short explanation as to why it is the ocrrect dataset
        """
    mod_query = f"I am looking for data products that can answer the query :{query}"
    config = {"filter": {"type": {"$eq": "product"}}}
    return getMultiLevelEvaluation(sys_prompt,config,mod_query)