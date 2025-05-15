
import json
import ast
import os
import yaml

from dotenv import load_dotenv

from langchain_postgres.vectorstores import PGVector
from langchain.agents import AgentExecutor, create_tool_calling_agent,tool
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain.tools.retriever import create_retriever_tool
from langchain_core.prompts import PromptTemplate,ChatPromptTemplate
from langchain_core.callbacks import UsageMetadataCallbackHandler

from Backend.Agent.transformations.execute import execute
from Backend.Agent import util
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
def getFunctionCatalog(type):
    """
        Retrieves the data catalog entry for a specific type of processing functions
        type: the type of function to retrieve aggregations or filter functions
        :return: dict containing all information about the data product
    """
    # TODO this should be a call to the microservice
    try:
        with open("../dataCatalog/configs/function_catalog.yml") as stream:
            catalog =  yaml.safe_load(stream)
    except FileNotFoundError:
        return "could not find the main catalog"

    try:
        return catalog[type]
    except KeyError:
        return catalog
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

    return llm.invoke(messages, config={"callbacks": [callback]})

def init_agent():
    sys_prompt = """ 
                Your task is to transform a user query into a list of api calls.
                To achieve your goal follo these steps:
                
                1. infer hich data products are needed ith the help of the retriever tool 
                2. create a sql query ith the createSQL tool
                3. transform the sql into a list of api call ith the help of the getFunctionCatalog tool to infear hich functions need to be called
                
                Retrieve additional information ith the getCatalogItem tool to make sure that all referenced columns are present in the data
                
                Example:
                user:"The mall ith the most items sold"
                output:  [{{"product":"http://127.0.0.1:5000/products/Sales_Data/sales_data_23","transformation":[{{"function":"http://127.0.0.1:5200/sum","values":{{"group_by":"shopping_mall","column":"quantity"}} }} ,{{"function":"http://127.0.0.1:5200/max","values":{{"column":"shopping_mall"}} }}]  }} ]
                
                Do only return the output list and no explanation
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
    tools = [retriever_tool, getFunctionCatalog, createSQLquery,getCatalogItem]

    agent = create_tool_calling_agent(llm, tools, prompt)
    return AgentExecutor(agent=agent, tools=tools, verbose=True)

agent = init_agent()
sql = "From the sales data i would like to know the total amount of money spent per category of available items, of Females over 38"
sql = " mall where the most toys where sold"
#agent_result = agent.invoke({'input':sql})['output']
agent_result = [{"product":"http://127.0.0.1:5000/products/Sales_Data/sales_data_23","transformation":[{"function":"http://127.0.0.1:5200/sum","values":{"group_by":"shopping_mall","column":"quantity"}}]}]
execute(agent_result)
print(agent_result)