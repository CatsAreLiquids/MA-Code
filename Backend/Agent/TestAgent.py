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
load_dotenv()

@tool
def functionRetriever(query):
    """
    Searches and returns Data files aboout the functions accesible for the processing
    :param query: the user query
    :return:
    """
    prompt = """You are an assistant for question-answering tasks. Use the following pieces of retrieved context to answer the question. If you don't know the answer, just say that you don't know. Use three sentences maximum and keep the answer concise.
        Question: {question} 
        Context: {context} 
        Answer:
    """

    config = {"filter":{"type":{"$eq":"function"}}}
    retrieval_chain = vector_db.getChain(prompt,config)
    return retrieval_chain.invoke(query)


@tool
def getCatalogItem(file):
    """
        Retrieves the data catalog entry for specific data product
        file: the file name of a specific data product
        :return: dict containing all information about the data product
    """
    # TODO this should be a call to the microservice
    try:
        with open("../data/Catalogs/catalog.yml") as stream:
            catalog = yaml.safe_load(stream)
    except FileNotFoundError:
        return "could not find the main catalog"

    for collection in catalog:
        if file in collection['products']:
            try:
                with open("../dataCatalog/Catalogs/" + collection['name'] + ".yml") as stream:
                    collection_dict = yaml.safe_load(stream)
                    for product in collection_dict['products']:
                        if product['name'] == file:
                            return product

            except FileNotFoundError:
                return "could not find the specific collection catalog"


@tool
def getCatalogColumns(file):
    """
        Retrieves the data catalog entry for specific data product
        file: the file name of a specific data product
        :return: dict containing all information about the data product
    """

    if 'http' in file:
        file = file.split("/")[-1]

    response = requests.get("http://127.0.0.1:5000/catalog", json={"file": file})
    return json.loads(response.text)

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

@tool
def confirmResult(query: str,result):
    """
    Breaks down a user query into multiple steps
    :param query user query asking for an data product
    :return: list of steps
    """
    if isinstance(result,pd.Series) or isinstance(result,pd.DataFrame):
        data = result[:5]
    else:
        data = result

    sys_prompt = """ Your task is to decide wether the provided data answers a query. 
                    For this you will receive at max the top 5 rows of the data, try to extrapolate if the data answers the query
                    Do only return True if the data seems to answer the query and return False, if the query is not answerd give a one sentence sumarry of what seems to be wrong
        """
    input_prompt = PromptTemplate.from_template("""
                User Query:{query}
                Data:{data}
                """)
    input_prompt = input_prompt.format(query=query,data= data)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages, config={"callbacks": [callback]})

@tool()
def createSQLquery(query):
    """
    stuff
    :return:
    """
    sys_prompt = """ Your task is to rewrite a user query into an sql query.
            """
    input_prompt = PromptTemplate.from_template("""
                    User Query:{query}
                    """)
    input_prompt = input_prompt.format(query=query)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    llm, callback = models.get_LLM_with_callbacks()
    return llm.invoke(messages, config={"callbacks": [callback]})


def init_agent():
    sys_prompt = """ 
                Your task is to transform a user query into a list of api calls.
                To achieve your goal follo these steps:
                
                1. infer hich data products are needed ith the help of the retriever tool 
                2. create a sql query ith the createSQL tool
                3. transform the sql into a list of api call ith the help of the getFunctionCatalog tool to infear hich functions need to be called
                
                Retrieve additional information ith the getCatalogItem tool to make sure that all referenced columns are present in the data
                Make sure that all columns referenced in a query are present in the data
                
                
                Example:
                user:"The mall ith the most items sold"
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
    tools = [retriever_tool,functionRetriever, getFunctionCatalog, createSQLquery, getCatalogColumns]
    llm, callback = models.get_LLM_with_callbacks()

    agent = create_tool_calling_agent(llm, tools, prompt)
    return AgentExecutor(agent=agent, tools=tools, verbose=True),callback


with get_openai_callback() as cb:
    agent_i,test = init_agent()
    sql = "From the sales data i would like to know the total amount of money spent per category of available items, of Females over 38"
    sql = "The year of the highest per GDP greenhouse gas emissions for Turkey"
    agent_result = agent_i.invoke({'input': sql})['output']
    agent_result = ast.literal_eval(agent_result)
    #agent_result = {'products': [{'product': 'http://127.0.0.1:5000/products/Sales_Data/sales_data_23', 'transformation': [{'function': 'http://127.0.0.1:5200/filter', 'columns': {'category': 'toys'}}, {'function': 'http://127.0.0.1:5200/sum', 'values': {'group_by': 'shopping_mall', 'column': 'quantity'}}, {'function': 'http://127.0.0.1:5200/max', 'values': {'column': 'shopping_mall'}}]}], 'combination': []}
    #agent_result = {"products":[{"product":"http://127.0.0.1:5000/products/Sales_Data/sales_data_23","transformation":[{"function":"http://127.0.0.1:5200/sum","values":{"group_by":"category","columns":"price"}}]},{"product":"http://127.0.0.1:5000/products/Sales_Data/customer_data_23","transformation":[{"function":"http://127.0.0.1:5200/filter","filter_dict":{"columns":{"gender":"Female","age":{"min":38}}}},{"function":"http://127.0.0.1:5200/getRows","filter_dict":{"columns":"customer_id","values":"None"}}]}],"combination":[{"column":"customer_id","type":"equals","values":["None"]}]}
    print(agent_result)
    print(execute.execute(agent_result))


#print(f"Total Tokens: {cb.total_tokens}")
#print(f"Prompt Tokens: {cb.prompt_tokens}")
#print(f"Completion Tokens: {cb.completion_tokens}")
#print(f"Total Cost (USD): ${cb.total_cost}")

print(test.usage_metadata)