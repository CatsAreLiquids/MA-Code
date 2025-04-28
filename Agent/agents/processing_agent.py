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
def extractKeywords(query):
    """
    Function to extract keywords out of a user message
    :return: a comma seperated list of keywords: [keyword1,keyword2,keyword3,...]
    """
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                json.load(open("../../prompts.json"))['tools']["extractKeywords"]
            ),
            ("user", query),
        ]
    )
    response = llm.invoke(prompt.format(input=query))
    return response.content

@tool
def matchFunction(query, catalog_dict):
    """
        Matches a user query to the fitting data product described in the data catalog
        query: user query
        catalog_dict: a data catalog item retrieved with getCatalogItem
        :return: a url of a specific dataproduct
    """
    # TODO would be really fancy if we can get to a point where stuf like relative growth per 5 year period
    # TODO also stuff  like average per substance is still difficult
    sys_prompt = """ Your task is to check weather the the query a user descibes can be solved with any of the apis provided.
                You only provide the base api or say "no fitting data product found please rephrase your query"
                For example
                query: Sum of Germanys C02 emissions with a 5 year period
                catalog_dict {products:[{"name":"sum","description":"calculates the sum of certein values","base_api":"http://127.0.0.1:5000/EDGAR_2024_GHG/product/sum"}
                return http://127.0.0.1:5000/EDGAR_2024_GHG/product/sum
                """
    input_prompt = PromptTemplate.from_template("""
        User Query:{query}
        catalog dict:{catalog_dict}
        """)
    input_prompt = input_prompt.format(query=query,catalog_dict=catalog_dict)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages)

def init_agent():
    prompt = ChatPromptTemplate.from_messages(
            [
                ("system",
                 "Your taks is to identify the necerssary steps and functions to transform data in the way a user describes."),
                ("human", "{input}"),
                ("placeholder", "{agent_scratchpad}"),
            ]
        )


    tools = [matchFunction, extractKeywords, getCatalogItem]

    agent = create_tool_calling_agent(llm, tools, prompt)

    return AgentExecutor(agent=agent, tools=tools, verbose=True)

#print(extractKeywords("All females customers who paid with Credit Card and are at least 38 years old"))