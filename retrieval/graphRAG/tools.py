from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain.agents import tool
import os
from dotenv import load_dotenv
from langchain_core.prompts import PromptTemplate
import stores
import json
from langchain_core.tools import StructuredTool
import requests
from langchain_postgres.vectorstores import PGVector
from langchain.tools.retriever import create_retriever_tool
#TODO structured tools for some of these

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


@tool
def retrieverTool():
    """
    Searches and returns Data files about the greenhouse gas emissions of diffrent countires.
    :return: relevant documents
    """
    return create_retriever_tool(
        vector_store.as_retriever(),
        "data_retriever",
        "Searches and returns Data files about the greenhouse gas emissions of diffrent countires.",
    )

@tool
def currentDate():
    """
    :return: the currentDate in the "YYYY-MM-DD" format
    """
    return date.today()


@tool
def extractKeywords(query):
    """
    Function to extract keywords out of a user message
    :return: a comma seperated list of keywords: [keyword1,keyword2,keyword3,...]
    """
    llm = AzureChatOpenAI(
        azure_endpoint=os.environ["GPT_EndPoint"],
        openai_api_version=os.environ["GPT_APIversion"],
    )
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                json.load(open("../prompts.json"))['tools']["extractKeywords"]
            ),
            ("user", query),
        ]
    )
    response = llm.invoke(prompt.format(input=query))
    return response.content

@tool
def getCatalogItem(file):
    """
        Retrieves the data catalog entry for specific data product
        file: the file name of a specific data product
        :return: dict containing all information about the data product
    """
    try:
        with open("../dataCatalog/configs/" + file +".yml") as stream:
            return yaml.safe_load(stream)
    except FileNotFoundError :
        return "could not find catalog Item stop execution"


@tool
def getDataProduct(base_api,filename, func, filter_dict):
    """
    Returns the full url including filters and base api for the user query
    base_api: a url for a specific data product
    filename: specific data to load
    func: any additional computations requested by the user set by the matchFunction tool
    filter_dict: specific filter based on the users input and created with the extractFilter tool

    :return: a url
    """

    filter_str = ""
    for key, value in filter_dict.items():
        filter_str += f"&{key}={value}"

    func_str = ""
    if func is not None:
        for key, value in filter_dict.items():
            func_str += f"&{key}={value}"
    else:
        func_str = "None&rolling=False&period=None"

    # TODO need o fix this somehow the dict doesnt want to process
    url = f"{base_api}?file={filename}&func={func}{filter_str}"

    return url

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

def extractFilter(query):
    """
    Creates a dict including filter atrributes and values for the getDataProduct tool
    query: the original user query
    :return: a dict with the corresponding filters such as dict={'Country1':'Austria','Country2':'Germany','min_year':1990,'max_year':1999}
    """
    sys_prompt = """You are a helpful assistent used to extract filters out of a user message.
                    Only return uniquly identifiable filters such as year spans or countries, aviod unclear filter like "co2 emissions"
                    Return all filter in a dict 
                    Example 
                    user query : "I would like to have the generall CO2 emissions data of Germany and Austria for the years 2000 to 2010"
                    response: {'Country1':'Austria','Country2':'Germany',"min_year":2000,"max_year":2010}
                    user query : "The Co2 data for Arubas building sector where the emissions are between 0.02 and 0.03"
                    response: {'Country1': 'Aruba', 'min_value': 0.02, 'max_value': 0.03}
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

def getTools():
    return [extractFilter,matchFunction,getDataProduct,getCatalogItem,extractKeywords]

