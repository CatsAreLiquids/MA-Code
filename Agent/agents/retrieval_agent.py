from langchain_core.messages import HumanMessage
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.agents import tool
import os
from dotenv import load_dotenv
import json
from langchain.agents import initialize_agent, Tool, AgentType

from langchain.chains import RetrievalQA

from langchain_postgres.vectorstores import PGVector
# import RAG
from langchain_core.prompts import PromptTemplate
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate
from langchain.tools.retriever import create_retriever_tool
from langgraph.prebuilt import create_react_agent

from datetime import date
import requests
import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
import re
import yaml

from sharedTool import getCatalogItem


load_dotenv()

embeddings = AzureOpenAIEmbeddings(
    model="text-embedding-ada-002",
    azure_endpoint=os.getenv("TextEmb_EndPoint")
)
llm = AzureChatOpenAI(
    azure_endpoint=os.environ["GPT_EndPoint"],
    openai_api_version=os.environ["GPT_APIversion"],
    model=os.environ["GPT_model_name"],
    deployment_name=os.environ["GPT_deployment"],
    temperature=0
)
connection = "postgresql+psycopg://langchain:langchain@localhost:6024/langchain"  # Uses psycopg3!
collection_name = "my_docs"

vector_store = PGVector(
    embeddings=embeddings,
    collection_name=collection_name,
    connection=connection,
    use_jsonb=True,
)

@tool
def getDataProduct(base_api,filename, func, filter_dict):
    """
    Returns the full url including filters and base api for the user query
    base_api: a url for a specific data product
    filename: specific data to load
    func: any additional computations requested by the user set by the matchFunction tool
    filter_dict: specific filter based on the users input and created with the extractFilter tool

    :return: a url
    """

    filter_str = ""
    for key, value in filter_dict.items():
        filter_str += f"&{key}={value}"

    func_str = ""
    if func is not None:
        for key, value in filter_dict.items():
            func_str += f"&{key}={value}"
    else:
        func_str = "None&rolling=False&period=None"

    # TODO need o fix this somehow the dict doesnt want to process
    url = f"{base_api}?file={filename}&func={func}{filter_str}"

    return url

def init_agent():
    prompt = ChatPromptTemplate.from_messages(
            [
                ("system",
                 "Your task is to help identify the correct api for a user based on their query"),
                ("human", "{input}"),
                ("placeholder", "{agent_scratchpad}"),
            ]
        )

    retriever_tool = create_retriever_tool(
        vector_store.as_retriever(),
        "data_retriever",
        "Searches and returns Data files aboout diffrent statistics ",
    )

    tools = [retriever_tool, getCatalogItem]

    agent = create_tool_calling_agent(llm, tools, prompt)

    return AgentExecutor(agent=agent, tools=tools, verbose=True)


@tool
def extractFilter(query, catalog_dict):
    """
    Creates a dict including filter atrributes and values for the getDataProduct tool
    query: the original user query
    catalog_dict: a dict containing information abpout the data product, can be retrieved with getCatalogItem
    :return: a dict with the corresponding filters such as dict={'Country1':'Austria','Country2':'Germany','min_year':1990,'max_year':1999}
    """
    sys_prompt = """Your task is to identify a filterable attribuites in a user query and find how they can be answerd with the data columns present in the data .
                    Only return uniquly identifiable filters such as year spans or countries, aviod unclear filter like "co2 emissions"
                    Return all filter in a dict 
                    Example 
                    user query : "I would like to have the generall CO2 emissions data of Germany and Austria for the years 2000 to 2010"
                    response: {'Country1':'Austria','Country2':'Germany',"min_year":2000,"max_year":2010}
                    user query : "The Co2 data for Arubas building sector where the emissions are between 0.02 and 0.03"
                    response: {'Country1': 'Aruba', 'min_value': 0.02, 'max_value': 0.03}
                    user query : "Customer sales data of women over 38 paying by credit card"
                    response: {'gender': 'Women', 'age': 38, 'pyament': credit card}

                    """
    input_prompt = PromptTemplate.from_template("""
            User Query:{query}
            Catalog Dict:{catalog_dict}
            """)
    input_prompt = input_prompt.format(query=query, catalog_dict=catalog_dict)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages)