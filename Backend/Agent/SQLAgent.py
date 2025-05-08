import re

import pandas as pd

from Backend.Agent import util
import json
import ast
import os
import yaml

from typing import List, Optional
from dotenv import load_dotenv

from langchain_postgres.vectorstores import PGVector
from langchain.agents import AgentExecutor, create_tool_calling_agent,tool
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain.tools.retriever import create_retriever_tool
from langchain_core.prompts import PromptTemplate,ChatPromptTemplate
from langchain_core.callbacks import UsageMetadataCallbackHandler

from Backend.Agent.transformations import aggregation
from Backend.Agent.transformations import filter
from Backend.Agent.transformations.execute import execute

import duckdb

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
    temperature=0,

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
def getCatalogItem(file):
    """
        Retrieves the data catalog entry for specific data product
        file: the file name of a specific data product
        :return: dict containing all information about the data product
    """
    # TODO this should be a call to the microservice
    try:
        with open("../dataCatalog/configs/catalog.yml") as stream:
            catalog = yaml.safe_load(stream)
    except FileNotFoundError:
        return "could not find the main catalog"

    for collection in catalog:
        if file in collection['products']:
            try:
                with open("../dataCatalog/configs/" + collection['name'] + ".yml") as stream:
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
    # TODO this should be a call to the microservice
    try:
        with open("../dataCatalog/configs/catalog.yml") as stream:
            catalog = yaml.safe_load(stream)
    except FileNotFoundError:
        return "could not find the main catalog"

    for collection in catalog:
        if file in collection['products']:
            try:
                with open("../dataCatalog/configs/" + collection['name'] + ".yml") as stream:
                    collection_dict = yaml.safe_load(stream)
                    for product in collection_dict['products']:
                        if product['name'] == file:
                            return product['columns']

            except FileNotFoundError:
                return "could not find the specific collection catalog"

@tool
def correctQUery():
    """pass"""
    pass

def init_planning_agent():
    sys_prompt = """ Your task is to rewrite a user query into an sql query.
                    To do this use the retriever_tool tool to find the corressponding data product and the getCatalogItem tool to gather all necerssary information about this data product
                    Make sure that the columns referenced in the sql query are realy in the data product
                    Return the result as fowllos:
                    {{"product":[data products names],"query": sql query}}
                    Example:
                    {{"product":[Actors_data,movie_data],"query": "SELECT avg(age) as age FROM Actors_data WHERE actor_id in (SELECT actor_id from movie_data WHERE release_year > 2020);}}
                    and replace data product name and sql query ith the corresponding values
        """

    retriever_tool = create_retriever_tool(
        vector_store.as_retriever(),
        "data_retriever",
        "Searches and returns Data files aboout diffrent statistics ",
    )

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", sys_prompt),
            ("human", "{input}"),
            ("placeholder", "{agent_scratchpad}"),
        ]
    )

    tools = [retriever_tool, getCatalogColumns,getCatalogItem]

    agent = create_tool_calling_agent(llm, tools, prompt)
    return AgentExecutor(agent=agent, tools=tools, verbose=True)

def _getRelevantColumns(query):
    comparisons = re.findall(r"WHERE(.*?)\s*(?:;|GROUP BY|ORDER BY|\sIN\s)",query)

    #TODO this could be better
    comparisons = [elem.split('AND') for elem in comparisons]
    comparisons = [x for xs in comparisons for x in xs]
    comparisons = [elem.split('OR') for elem in comparisons]
    comparisons = [x for xs in comparisons for x in xs]

    columns = []
    for elem in comparisons:
        if '=' in elem :
            columns.append(elem.rsplit('=')[0].strip())

    return columns

def getDF(name):
    location = util.getProductKey(name,'location')
    try :
        return pd.read_csv(location)
    except:
        return "File not found"

def formatSQL(result):
    str_pattern = 'df{}'
    for i, product in enumerate(result['product'], start=1):
        name = str_pattern.format(i)
        globals()[name] = getDF(product)

        result['query'] = result['query'].replace(product,name)
    return result

def runSQL(result):

    q = formatSQL(result)['query']
    return duckdb.sql(q).df()


def rerunQuery(query, result):

    relevant_columns = set(_getRelevantColumns(result['query']))
    all_columns = [util.getProductKey(name,'columns') for name in result['product']]

    res = {}
    for i in range(len(all_columns)):
        for comp in relevant_columns:
            if comp in all_columns[i]:
                df = util.getData(util.getProductKey(result['product'][i],'base_api'))
                res[comp] = df[comp].unique().tolist()

    return res

def runQuery():
    callback = UsageMetadataCallbackHandler()

def _correctQuery(query,agent_result):
    correctioins = rerunQuery(query, agent_result)
    modded_query = f"The original query:\n {query}\n Produced no results try correct it ith the list of possible values for the columns {correctioins}"
    agent_result = agent.invoke({'input': modded_query}, config={"callbacks": [callback]})
    agent_result = ast.literal_eval(agent_result['output'])
    return runSQL(agent_result)

def runQueryRemote(query):
    callback = UsageMetadataCallbackHandler()
    agent = init_planning_agent()
    agent_result = agent.invoke({'input': query}, config={"callbacks": [callback]})
    agent_result = ast.literal_eval(agent_result['output'])

    df = runSQL(agent_result)
    if df.empty:
        correctioins = rerunQuery(query, agent_result)
        modded_query = f"The original query:\n {query}\n Produced no results try correct it ith the list of possible values for the columns {correctioins}"
        agent_result = agent.invoke({'input': modded_query}, config={"callbacks": [callback]})
        agent_result = ast.literal_eval(agent_result['output'])
        df=  runSQL(agent_result)

    return df

if __name__ == "__main__":
    user = "From the sales data i would like to know the total amount of money spent per category of available items, of Females over 38"
    user = "Germanies total emissons for the 2000s"
    #agent_result = {'product': ['sales_data_23','customer_data_23'], 'query': "SELECT category, SUM(price * quantity) AS total_spent FROM sales_data_23 WHERE customer_id IN (SELECT customer_id FROM sales_data_23 WHERE age > 38 AND gender = 'Female') GROUP BY category;"}
    agent = init_planning_agent()
    agent_result = agent.invoke({'input':user})['output']
    agent_result = ast.literal_eval(agent_result)
    #agent_result = {'product': ['sales_data_23'], 'query': "SELECT customer_id, COUNT(invoice_no) as number_of_purchases FROM sales_data_23 WHERE category = 'toy' GROUP BY customer_id HAVING COUNT(invoice_no) > 0 ORDER BY number_of_purchases DESC;"}
    #print(callback.usage_metadata)
    #print(agent_result)
    print(runSQL(agent_result))
    #runSQL(agent_result)
    #print(runQuery(user))