from Agent.agents import processing_agent, retrieval_agent

import util
import json
import ast
import os
from transformations import aggregation
from transformations import filter

from langchain_postgres.vectorstores import PGVector

from langchain_core.prompts import PromptTemplate
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate
from dotenv import load_dotenv
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain.agents import tool
import yaml

from langchain_core.tools import StructuredTool
from langchain.tools.retriever import create_retriever_tool
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

aggregations = {'sum': aggregation.getSum, "mean": aggregation.mean}
filters = {'getRows': filter.getRows, 'filter': filter.applyFilter, 'combine': filter.combineProducts}


@tool
def createExecutionPlan(query:str, dataProduct):
    """
    Creates an execution plan based on the input
    :param query: a query extracted from the original user input with breakDownQuery
    :param dataProduct: name of the data product ( by identifyDataProduct )
    :return: [{{"function":"filter","values":{{"gender":"Female","age":{{"min":38,"max":38}} }} }},{{"function":"getRows","values":{{"customer_id":"None"}} }}]
    """
    query = query + f"The correct data products name is {dataProduct}"
    pagent = processing_agent.init_agent()
    agent_result = pagent.invoke({"input": query})['output']
    agent_result = ast.literal_eval(agent_result)
    return agent_result

@tool
def identifyDataProduct(query:str):
    """
    Calls a retriever agent that identfies the most fitting data product for the input query
    :param query: a user query defining a specifc data product
    :return: {{"name":"name","url":"http://127.0.0.1:5000/exampleUrl"}}
    """
    ragent = retrieval_agent.init_agent()
    agent_result = ragent.invoke({"input": query})['output']
    agent_result = json.loads(agent_result)

    return agent_result
#TODO @tool(parse_docstring=True)
@tool
def breakDownQuery(query:str):
    """
    Breaks down a user query into multiple steps
    :param query user query asking for an data product
    :return: list of steps
    """
    sys_prompt = """ Your task is to deconstruct a user query into multipe parts if necerssary.
                    Example
                    User query: Based on the customer data of women over 38 who have paid with credit cards I want to see the sum of all sales per category in the sales data
                    parts:["customer data of women over 38 who have paid with credit cards", "the sum of all sales per category in the sales data"]

                    User query: Average age of all customers who previously have bought toys, by gender
                    parts:["Customers who have bought toys", "Average age by gender"]

                    User query: "All females customers who paid with Credit Card and are at least 38 years old"
                    parts:["Females who paid ith credit card over 38"]


        """
    input_prompt = PromptTemplate.from_template("""
                User Query:{query}
                """)
    input_prompt = input_prompt.format(query=query)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages)


def init_tools():
    return [identifyDataProduct,createExecutionPlan,breakDownQuery]
