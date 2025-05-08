import pandas as pd

import util
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

from transformations import aggregation
from transformations import filter
from transformations.execute import execute

from pandasql import sqldf

load_dotenv()

callback = UsageMetadataCallbackHandler()

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
def identifyDataProduct(query: str):
    """
    Calls a retriever agent that identfies the most fitting data product for the input query
    :param query: a user query defining a specifc data product
    :return: {{"name":"name","url":"http://127.0.0.1:5000/exampleUrl"}}
    """
    ragent = init_retrieval_agent()
    agent_result = ragent.invoke({"input": query}, config={"callbacks": [callback]})['output']
    agent_result = json.loads(agent_result)

    return agent_result

def init_retrieval_agent():
    sys_prompt = """ Your task is to help identify the correct url and data product for a user based on their query
                    Only provide one url at a time together with the the name of the data product.
                    The output should be a valid json formatted as follwos:
                    {{"name":"name","url":"http://127.0.0.1:5000/exampleUrl"}}
    """
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", sys_prompt),
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

def init_planning_agent():
    sys_prompt = """ Your task is to rewrite a user query into an sql query.
                    To do this use the identifyDataProduct tool to find the corressponding data product and the getCatalogItem tool to gather all necerssary information about this data product
                    Return the result as fowllos:
                    {{"product":data product name,"query": sql query}}
                    and replace data product name and sql query ith the corresponding values
        """
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", sys_prompt),
            ("human", "{input}"),
            ("placeholder", "{agent_scratchpad}"),
        ]
    )

    tools = [identifyDataProduct, getCatalogItem]

    agent = create_tool_calling_agent(llm, tools, prompt)
    return AgentExecutor(agent=agent, tools=tools, verbose=True)

def runSQL(result):

    df = pd.read_csv("../data/EDGAR_2024_GHG/GHG_totals_by_country.csv")
    q = result['query']
    q = """ SELECT Country, "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009" FROM df WHERE Country = "Germany"  """

    print(sqldf(q, locals()))

if __name__ == "__main__":
    user = "From the sales data i would like to know the total amount of money spent per category of available items, of Females over 38"
    agent = init_planning_agent()
    agent_result = agent.invoke({'input':user},config={"callbacks": [callback]})['output']
    agent_result = ast.literal_eval(agent_result)
    print(agent_result)
    agent_result = {'product': 'GHG_totals_by_country', 'query': 'SELECT Country, "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009"  WHERE Country = Germany'}
    print(agent_result)
    runSQL(agent_result)
    #print(execute(agent_result))
    print(callback.usage_metadata)