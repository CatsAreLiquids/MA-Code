import ast
import os

import pandas as pd
import yaml
import requests
import json

from dotenv import load_dotenv
from langchain_postgres.vectorstores import PGVector
from langchain.agents import AgentExecutor, create_tool_calling_agent, tool
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain.tools.retriever import create_retriever_tool
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate
from langchain_core.callbacks import UsageMetadataCallbackHandler
from langchain_community.callbacks import get_openai_callback
from Backend import models
from Backend.Agent import execute
from Backend.RAG import vector_db


@tool(parse_docstring=True)
def functionRetriever(query):
    """
    Searches and returns Data files aboout the functions accesible for the processing
    Args:
        query:describtion of the functions needed
    Returns:
        a text descrbing potential functions
    """
    prompt = """You are an assistant for question-answering tasks. Use the following pieces of retrieved context to answer the question. If you don't know the answer, just say that you don't know. Use three sentences maximum and keep the answer concise.
        Question: {question} 
        Context: {context} 
        Answer:
    """

    config = {"filter":{"type":{"$eq":"function"}}}
    retrieval_chain = vector_db.getChain(prompt,config)
    return retrieval_chain.invoke(query)

@tool(parse_docstring=True)
def productRetriever(query):
    """
    Returns the describtion of suitable dataproducts to answer the query
    Args:
        query: original user query
    Returns:
        a text descrbing potential functions
    """
    prompt = """You are an assistant for question-answering tasks. Use the following pieces of retrieved context to answer the question. If you don't know the answer, just say that you don't know. Use three sentences maximum and keep the answer concise.
        Question: {question} 
        Context: {context} 
        Answer:
    """

    config = {"filter":{"type":{"$eq":"product"}}}
    retrieval_chain = vector_db.getChain(prompt,config)
    return retrieval_chain.invoke(query)

@tool(parse_docstring=True)
def getCatalogItem(file):
    """
        Retrieves the data catalog entry for specific data product
        Args:
            file: the file name of a specific data product
        Returns: dict containing all information about the data product
    """

    if 'http' in file:
        file = file.split("/")[-1]

    response = requests.get("http://127.0.0.1:5000/catalog", json={"file": file})
    return json.loads(response.text)

@tool(parse_docstring=True)
def createSQLquery(query,columns):
    """
    Creates a sql query based on the provide user query and the column names in accesible data products
    Args:
        query: original user query
        columns: columns present in the data products
    Returns:
        a string sql query
    """
    sys_prompt = """ Your task is to rewrite a user query into an sql query.
            """
    input_prompt = PromptTemplate.from_template("""
                    User Query:{query}
                    Columns present: {columns}
                    """)
    input_prompt = input_prompt.format(query=query,columns=columns)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    llm = models.get_LLM()
    return llm.invoke(messages)

@tool
def getFunctionCatalog(name):
    """
        Retrieves the data catalog entry for a specific type of processing functions
        name: the function name to retrieve
        :return: dict containing all information about the data product
    """
    # TODO this should be a call to the microservice
    response = requests.get("http://127.0.0.1:5200/catalog",json={"function_name":name})
    return json.loads(response.text)

@tool()
def breakDownSQLQuery(sqlquery: str):
    """
    Breaks down a sql query into its multiple steps
    Args:
        sqlquery: a sql query
    Returns:
        a list of steps
    """
    sys_prompt = """ Your task is to deconstruct a sql query into multipe parts if necerssary.
    
                Example:
                query: "SELECT category, SUM(price) AS total_amount_spent\nFROM your_table_name\nWHERE gender = 'Female' AND age > 38\nGROUP BY category;"
                output: ["SELECT year, Türkiye FROM emissions_data", "WHERE Türkiye = (SELECT MAX(Türkiye) FROM emissions_data)"]
        """
    input_prompt = PromptTemplate.from_template("""
                User Query:{query}
                """)
    input_prompt = input_prompt.format(query=query)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]

    llm = models.get_LLM()

    return llm.invoke(messages)


def init_agent():
    sys_prompt = """ 
                Your task is to transform a user query into a list of api calls.
                To achieve your goal follo these steps:

                Example:
                user:"The mall with the most items sold"
                output:  {{"products":[{{"product":"http://127.0.0.1:5000/products/Sales_Data/sales_data_23","transformation":[{{"function":"http://127.0.0.1:5200/sum","values":{{"group_by":"shopping_mall","column":"quantity"}} }} ,{{"function":"http://127.0.0.1:5200/max","values":{{"column":"shopping_mall"}} }}]  }}], "combination":[] }}
                user: "All customer data of customers who have bought at least 1 book"
                output: {{"products":[{{"product":"http://127.0.0.1:5000/products/Sales_Data/sales_data_23","transformation":[{{"function":"http://127.0.0.1:5200/filter","columns":{{"category":"books"}} }} ,{{"function":"http://127.0.0.1:5200/getRows",filter_dict:{{"values":"None","column":"customer_id"}} }}]  }} ,{{"product":"http://127.0.0.1:5000/products/Sales_Data/customer_data_23","transformation":[]}}], "combination":[{{"column":"customer_id","type":"equals","values":["None"]}}] }}

                Do only return the output list and no explanation
        """
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", sys_prompt),
            ("human", "{input}"),
            ("placeholder", "{agent_scratchpad}"),
        ]
    )
    vector_store = models.getVectorStore()
    retriever_tool = create_retriever_tool(
        vector_store.as_retriever(),
        "data_retriever",
        "Searches and returns Data files aboout diffrent statistics ",
    )
    tools = [retriever_tool, functionRetriever, getFunctionCatalog, createSQLquery, getCatalogItem]#,breakDownSQLQuery]
    llm, callback = models.get_LLM_with_callbacks()

    agent = create_tool_calling_agent(llm, tools, prompt)
    return AgentExecutor(agent=agent, tools=tools, verbose=True), callback

with get_openai_callback() as cb:
    agent_i,test = init_agent()
    sql = "From the sales data i would like to know the total amount of money spent per category of available items, of Females over 38"
    #sql = "The year of the highest per GDP greenhouse gas emissions for Turkey"
    #sql = "The mall with the highest book sales"
    agent_result = agent_i.invoke({'input': sql})['output']
    agent_result = ast.literal_eval(agent_result)
    #agent_result = {'products': [{'product': 'http://127.0.0.1:5000/products/Sales_Data/sales_data_23', 'transformation': [{'function': 'http://127.0.0.1:5200/filter', 'columns': {'category': 'toys'}}, {'function': 'http://127.0.0.1:5200/sum', 'values': {'group_by': 'shopping_mall', 'column': 'quantity'}}, {'function': 'http://127.0.0.1:5200/max', 'values': {'column': 'shopping_mall'}}]}], 'combination': []}
    #agent_result = {"products":[{"product":"http://127.0.0.1:5000/products/Sales_Data/sales_data_23","transformation":[{"function":"http://127.0.0.1:5200/sum","values":{"group_by":"category","columns":"price"}}]},{"product":"http://127.0.0.1:5000/products/Sales_Data/customer_data_23","transformation":[{"function":"http://127.0.0.1:5200/filter","filter_dict":{"columns":{"gender":"Female","age":{"min":38}}}},{"function":"http://127.0.0.1:5200/getRows","filter_dict":{"columns":"customer_id","values":"None"}}]}],"combination":[{"column":"customer_id","type":"equals","values":["None"]}]}
    #agent_result ={"products": [{"product": "http://127.0.0.1:5000/products/Sales_Data/sales_data_23", "transformation": [{"function": "http://127.0.0.1:5200/filter", "columns": {"gender": "Female", "age": {"min": 39}}},{"function": "http://127.0.0.1:5200/sum", "values": {"group_by": "category", "column": "price"}}]}, {"product": "http://127.0.0.1:5000/products/Sales_Data/customer_data_23","transformation": []}], "combination": [{"column": "customer_id", "type": "equals", "values": ["None"]}]}

    print(agent_result)
    #print(execute.execute(agent_result))