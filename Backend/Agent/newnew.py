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
def getFunctionCatalog(func_name):
    """
    Retrieves the function catalog entry with further information
    Args:
        func_name: the name of a function
    Returns:
         a dict describing the function
    """

    response = requests.get("http://127.0.0.1:5200/catalog", json={"function_name": file})
    return json.loads(response.text)

@tool(parse_docstring=True)
def getCatalogColumns(file):
    """
    Retrieves the data catalog entry for specific data product
    Args:
        file: the file name of a specific data product
    Returns:
         a list of all column names in the data product
    """

    if 'http' in file:
        file = file.split("/")[-1]

    response = requests.get("http://127.0.0.1:5000/catalog/columns", json={"file": file})
    return json.loads(response.text)


@tool(parse_docstring=True)
def functionRetriever(step):
    """
    Searches and returns Data files aboout the functions accesible for the processing. Only usefull when trying to identify a function
    Args:
        step: a specific step in an execution plan, describing some kind of agreggation,transformation or filtering
    Returns:
        a text descrbing potential functions
    """
    prompt = """Your task is to find a the top 2 fitting functions that could solve problem described in the provided step.
                Provide the most fitting function and its full description. Your answer is mewant to be short and precise exclude any additional examples.
                
                Context: {context} 
                Answer:
    """
    mod_query = f"I am looking for a function that can solve the following problem for me :{step}"
    config = {"filter":{"type":{"$eq":"function"}}}
    retrieval_chain = vector_db.getChain(prompt,config)
    return retrieval_chain.invoke(mod_query)


@tool(parse_docstring=True)
def productRetrieverStep(query, step):
    """
    Returns the describtion of a suitable dataproducts to answer the query. Only usefull when trying to retrieve data
    Args:
        query: a natural languge user query
        step: a specific retrieval step that is being solved
    Returns:
        a text descrbing potential functions
    """
    prompt = """Your task is to help find the best fitting data product. You are provided with a user query and a specific product that is searched for.
                Provide the most likely fitting data products, always provide the data products name and why it would fit this query
                
        Question: {question} 
        Context: {context} 
        Answer:
    """
    mod_query = f"I am looking for data products that can answer the query: '{query}'\n Specifically the retrieval question {step} "
    config = {"filter": {"type": {"$eq": "product"}}}

    retrieval_chain = vector_db.getChain(prompt,config)
    return retrieval_chain.invoke(mod_query)

@tool(parse_docstring=True)
def productRetriever(query):
    """
    Returns the describtion of a suitable dataproducts to answer the query. Only usefull when trying to retrieve data
    Args:
        query: a natural languge user query
    Returns:
        a text descrbing potential functions
    """
    prompt = """Your task is to help find the best fitting data product. You are provided with a user query .
                Provide the most likely fitting data products, always provide the data products name and why it would fit this query
        Question: {question} 
        Context: {context} 
        Answer:
    """
    mod_query = f"I am looking for data products that can answer the query: '{query}'"
    config = {"filter": {"type": {"$eq": "product"}}}

    retrieval_chain = vector_db.getChain(prompt,config)
    return retrieval_chain.invoke(mod_query)


@tool(parse_docstring=False)
def breakDownQuery(query: str,prod_descriptions):
    """
    Breaks down a user query into its multiple steps
    Args:
        query:  a natural languge query
        prod_descriptions: information about potentially fitting data products
    Returns:
        a list of steps necerssary to solve the query
    """
    sys_prompt = """ Your task is to explain how you would slove the provided query. For this break it down into sepereate retrieval, computation and combination steps.
                You will be provided with information for fitting data products, keep the steps short and preciese.
                Return the steps as a orderd list.
                Example: All customer data of customers who have bought at least 1 book
                result ["retrieve sales data","filter for customers who have bought 1 book","retrieve customer data","combine customers from sales data and custmer data"] 
        """
    input_prompt = PromptTemplate.from_template("""
                User Query:{query}
                prod_describtions:{prod_descriptions}
                """)
    input_prompt = input_prompt.format(query=query,prod_descriptions=prod_descriptions)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]

    llm = models.get_LLM()

    return llm.invoke(messages)

@tool(parse_docstring=False)
def transformStep(step: str,func_description:str,columns:list[str]):
    """
    Breaks down a user query into its multiple steps
    Args:
        step:  a step in a query that is being solved
        func_description: a describtion of the function, get it from the functionRetriever tool
        columns: a list of columns in a data product that is needed to solve step
    Returns:
        a dict containing a function api and its parameters
    """
    sys_prompt = """ Your task is to combine all previously gatherd data into a preset output. use the following examples as a guide:
                     Mke sure that you only use 
                    Example:
                    step: aggregate total items sold for each mall
                    func_description: 
                    **Description**: This function calculates the sum of specified columns in a dataframe, optionally grouping the results by one or more columns. It requires a list of columns for which the sum should be calculated (cannot be None) and allows for grouping by a list of columns (can be None).
                    **API Endpoint**: [http://127.0.0.1:5200/sum](http://127.0.0.1:5200/sum)
                    **Filter Dictionary**:
                    - `group_by`: A list of columns in the dataframe (can be None).
                    - `columns`: A list of columns for which the sum should be calculated (cannot be None).
                    columns: ["invoice_no","customer_id","category","quantity","price","invoice_date","shopping_mall"]
                    
                    result : {{"function":"http://127.0.0.1:5200/sum","values":{{"group_by":"shopping_mall","column":"quantity"}}
                    
                    Example:
                    step: identify the mall with the highest total items sold
                    func_description:
                    **Description**: The `max` function retrieves the maximum value for a specified list of columns in a dataframe. It requires a filter dictionary that includes the columns for which the maximum values should be retrieved.
                    **API Endpoint**: `http://127.0.0.1:5200/max`
                    **Filter Dictionary**: 
                    - `columns`: A list of columns for which the maximum should be retrieved.
                    columns: ["invoice_no","customer_id","category","quantity","price","invoice_date","shopping_mall"]
                    
                    result : {{"function":"http://127.0.0.1:5200/max","values":{{"column":"shopping_mall"}} }}
                    
                    Example:
                    step: retrieve sales data from sales_data_23
                    func_description:
                    The most fitting function is **retrieve**. 
                    **Description**: The `retrieve` function retrieves the specified data product by taking the data product's name and a list of requested columns as arguments. It allows users to access specific data points efficiently. 
                    **Filter Dictionary**: 
                    - `{'product': 'api of the data product'}`
                    - `{'columns': 'list of columns to retrieve'}`
                    columns: ["invoice_no","customer_id","category","quantity","price","invoice_date","shopping_mall"]
                    
                    result : {{"function":"http://127.0.0.1:5200/retrieve","values":{{"columns":["invoice_no","customer_id","category","quantity","price","invoice_date","shopping_mall"],"product":"http://127.0.0.1:5000/products/Sales_Data/customer_data_23" }} }}
        """
    input_prompt = PromptTemplate.from_template("""
                step:{step}
                func_description: {func_description}
                columns: {columns}
                """)
    input_prompt = input_prompt.format(step=step,func_description=func_description,columns=columns)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]

    llm = models.get_LLM()

    return llm.invoke(messages)


def init_agent():
    sys_prompt = """ Your task is to create an execution plan for a user query.
                    For this use the provided tools to 
                    1. identify the necerssary data products using the productRetriever tool
                    2. break down the query into its sub steps
                    3. for each substep identfiy the necerssary function call using the functionRetriever tool
                    4. transfrom the output to fit the schema
                
                    Example:
                    user:"The mall ith the most items sold"
                    output:  {{"plans":[[{{"function":"http://127.0.0.1:5200/retrieve","values":{{"product":"http://127.0.0.1:5000/products/Sales_Data/sales_data_23","columns":["invoice_no","customer_id","category","quantity","price","invoice_date","shopping_mall"]}} }},{{"function":"http://127.0.0.1:5200/filter","values":{{"columns":"category","criteria":{{"category":"book"}} }} }},{{"function":"http://127.0.0.1:5200/sum","values":{{"group_by":"shopping_mall","column":"quantity"}} }},{{"function":"http://127.0.0.1:5200/sortby","values":{{"columns":"quantity","order":"descending"}} }}]],"combination":[]}}
                    user: "All customer data of customers who have bought at least 1 book"
                    output: {{"plans":[[{{"function":"http://127.0.0.1:5200/retrieve","values":{{"product":"http://127.0.0.1:5000/products/Sales_Data/customer_data_23","columns":["customer_id","category"]}} }},{{"function":"http://127.0.0.1:5200/filter","values":{{"columns":{{"category":"book"}} }} }},{{"function":"http://127.0.0.1:5200/getRows","values":{{"filter_dict":{{"values":"None","column":"customer_id"}} }} }}],[{{"function":"http://127.0.0.1:5200/retrieve","values":{{"product":"http://127.0.0.1:5000/products/Sales_Data/sales_data_23","columns":["customer_id","gender","age","payment_method"]}} }}]],"combination":[{{"column":"customer_id","type":"equals","values":["None"]}} ] }}
                    Do only return the result, keep to the provided schema, and do not explain it 
        """
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", sys_prompt),
            ("human", "{input}"),
            ("placeholder", "{agent_scratchpad}"),
        ]
    )

    tools = [breakDownQuery, getCatalogColumns,functionRetriever,productRetriever,transformStep,getFunctionCatalog]

    agent = create_tool_calling_agent(models.get_LLM(), tools, prompt)
    return AgentExecutor(agent=agent, tools=tools, verbose=True)


print(functionRetriever.invoke("filter sales data for the book category"))
#The mall with the most items sold
#print(breakDownQuery.invoke("The ten countries with the highest per capita emissions for 2007"))
# retrieve sales data from all malls
#print(productRetriever.invoke({"query":"The mall ith the most items sold","step":"retrieve sales data from all malls"}))
#aggregate total items sold for each mall
#identify the mall with the highest total items sold
#print(functionRetriever.invoke({"step":"filter sales data to include only book sales"}))
#TODO solve combine issue --> is returned when not fitting
#TODO filter tool keeps doing ">38"
#TODO does not call function retriever
#TODO rework transform step cause weird
#TODO combine is still a problem

#'retrieve sales data from sales_data_23'
#'filter sales data for book category'
#'identify the mall with the highest total book sales'
#{"plans":[[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/Sales_Data/sales_data_23","columns":["invoice_no","customer_id","category","quantity","price","invoice_date","shopping_mall"]}},{"function":"http://127.0.0.1:5200/filter","values":{"columns":["category"],"values":["books"]}},{"function":"http://127.0.0.1:5200/sum","values":{"group_by":"shopping_mall","column":"quantity"}},{"function":"http://127.0.0.1:5200/sortby","values":{"columns":["quantity"],"order":"descending"}}]],"combination":[]}}

"""
with get_openai_callback() as cb:
    agent_i = init_agent()
    sql = "From the sales data i would like to know the total amount of money spent per category of available items, of Females over 38"
    #sql = "The year of the highest per GDP greenhouse gas emissions for Turkey"
    sql = "The mall with the highest book sales"
    #sql = "All customer data of customers who have bought at least 1 book"
    agent_result = agent_i.invoke({'input': sql})['output']
    print(agent_result)
    agent_result = ast.literal_eval(agent_result)
    #print(execute.execute(agent_result))
"""