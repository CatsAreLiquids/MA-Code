import os
import yaml

from dotenv import load_dotenv
from langchain_postgres.vectorstores import PGVector
from langchain.agents import AgentExecutor, create_tool_calling_agent,tool
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain.tools.retriever import create_retriever_tool
from langchain_core.prompts import PromptTemplate,ChatPromptTemplate
from langchain_core.callbacks import UsageMetadataCallbackHandler

from Backend import models
load_dotenv()

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
        with open("../data/Catalogs/function_catalog.yml") as stream:
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
    llm,callback = models.get_LLM_with_callbacks()
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
                output:  {{"products":[{{"product":"http://127.0.0.1:5000/products/Sales_Data/sales_data_23","transformation":[{{"function":"http://127.0.0.1:5200/sum","values":{{"group_by":"shopping_mall","column":"quantity"}} }} ,{{"function":"http://127.0.0.1:5200/max","values":{{"column":"shopping_mall"}} }}]  }}], "combination":{{}} }}
                user: "All customer data of customers who have bought at least 1 book"
                output: {{"products":[{{"product":"http://127.0.0.1:5000/products/Sales_Data/sales_data_23","transformation":[{{"function":"http://127.0.0.1:5200/filter","columns":{{"category":"books"}} }} ,{{"function":"http://127.0.0.1:5200/getRows",filter_dict:{{"values":"None","column":"customer_id"}} }}]  }} ,{{"product":"http://127.0.0.1:5000/products/Sales_Data/customer_data_23","transformation":[]}}], "combination":{{"column":"customer_id","type":"equals","values":["None"]}} }}
                
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
    tools = [retriever_tool, getFunctionCatalog, createSQLquery,getCatalogColumns]
    llm,callback = models.get_LLM_with_callbacks()

    agent = create_tool_calling_agent(llm, tools, prompt)
    return AgentExecutor(agent=agent, tools=tools, verbose=True)

agent = init_agent()
sql = "From the sales data i would like to know the total amount of money spent per category of available items, of Females over 38"
#sql = " mall where the most toys where sold"
agent_result = agent.invoke({'input':sql})['output']
#agent_result = {"products":[{"product":"http://127.0.0.1:5000/products/Sales_Data/sales_data_23","transformation":[{"function":"http://127.0.0.1:5200/sum","values":{"group_by":"category","column":"amount_spent"}},{"function":"http://127.0.0.1:5200/filter","columns":{"customer_id":{"in":"(SELECT customer_id FROM customer_data_23 WHERE gender = 'Female' AND age > 38)"}}}]}},{"product":"http://127.0.0.1:5000/products/Sales_Data/customer_data_23","transformation":[]}],"combination":{}}
#execute(agent_result)
print(agent_result)