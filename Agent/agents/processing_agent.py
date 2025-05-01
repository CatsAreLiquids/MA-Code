from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain.agents import tool
import os
from dotenv import load_dotenv
import json

from langchain_postgres.vectorstores import PGVector
# import RAG
from langchain_core.prompts import PromptTemplate
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate

from Agent.sharedTool import getCatalogItem


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
                    response: {'Country':['Austria','Germany'],"year":{"min":2000,"max":2010} }
                    user query : "The Co2 data for Aruba where the emissions are between 0.02 and 0.03, from 1990 onward"
                    response: {'Country': 'Aruba', "value":{'min': 0.02, 'max': 0.03} "year":{'min':1990} }
                    user query : "Customer sales data of women over 38 paying by credit card"
                    response: {'gender': 'Women', 'age':{'min':38,'max':38} , 'pyament': "credit card"}
                    user query : "Customers who have not payed with creddit card"
                    response: {'gender': 'Women' , 'pyament': {'not':"credit card"}}
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

def init_agent():
    sys_prompt = """ Based on a user query identifiy the nercerssary steps and fucntions to transform data accordingly.
                    You only need to identify which functions need to be used and with which parameters. Use the available tools
                    The output should be a string containing a list of valid of python dictionaires, each dict containing one execution step and the necerssary values:
                    User query: "All females customers who paid with Credit Card and are at least 38 years old"
                    response [{{"function":"filter","values":{{"gender":"Female","age":{{"min":38,"max":38}} }} }},{{"function":"getRows","values":{{"customer_id":"None"}} }}]
                    Do not return a json
        """
    prompt = ChatPromptTemplate.from_messages(
            [
                ("system",sys_prompt),
                ("human", "{input}"),
                ("placeholder", "{agent_scratchpad}"),
            ]
        )


    tools = [matchFunction, extractFilter, getCatalogItem]

    agent = create_tool_calling_agent(llm, tools, prompt)

    return AgentExecutor(agent=agent, tools=tools, verbose=True)