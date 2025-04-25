from langchain_core.messages import HumanMessage
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.agents import tool
import os
from dotenv import load_dotenv
import json
from langchain.agents import initialize_agent, Tool, AgentType

from langchain.chains import RetrievalQA

from langchain_postgres.vectorstores import PGVector
# import RAG
from langchain_core.prompts import PromptTemplate
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate
from langchain.tools.retriever import create_retriever_tool
from langgraph.prebuilt import create_react_agent

from datetime import date
import requests
import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
import re
import yaml


class ResponseFormat(BaseModel):
    """Respond to the user in this format."""
    API: float = Field(description="LINK to find the data at")


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

productList = ['EDGAR_2024_GHG']


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


@tool
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

def init_agent():
    agent_prompt = "You are a helpful assistant providing users with their requested data via providing opnly the link the data can be found at. "
    agent_prompt = """You are a helpful assistant providing users with their requested data via providing a url.
                        Additionnaly provide a variable named multiple=True if multiple products are needed to answer the question, otherwise multiple=False 
                        The output should look like:
                         Url: url 
                         multiple"""

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", agent_prompt),
            #MessagesPlaceholder("chat_history", optional=True),
            ("human", "{input}"),

            # Placeholders fill up a **list** of messages
            ("placeholder", "{agent_scratchpad}"),
        ]
    )

    retriever_tool = create_retriever_tool(
        vector_store.as_retriever(),
        "data_retriever",
        "Searches and returns Data files about the greenhouse gas emissions of diffrent countires.",
    )
    #memory = ConversationBufferMemory(memory_key="chat_history", return_messages=True)
    tools = [retriever_tool, getDataProduct, matchFunction, extractFilter]

    agent = create_tool_calling_agent(llm, tools, prompt)
    agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=False, response_format=ResponseFormat)

    return agent_executor

def remoteChat(message):
    # TODO fix agent output
    agent_prompt = "You are a helpful assistant providing users with their requested data via providing opnly the link the data can be found at. "
    agent_prompt = """You are a helpful assistant providing users with their requested data via providing a url.
                    Additionnaly provide a variable named multiple=True if multiple products are needed to answer the question, otherwise multiple=False 
                    The output should look like:
                     Url: url 
                     multiple"""

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", agent_prompt),
            ("human", "{input}"),

            # Placeholders fill up a **list** of messages
            ("placeholder", "{agent_scratchpad}"),
        ]
    )

    retriever_tool = create_retriever_tool(
        vector_store.as_retriever(),
        "data_retriever",
        "Searches and returns Data files about the greenhouse gas emissions of diffrent countires.",
    )

    tools = [retriever_tool, getDataProduct, matchFunction, extractFilter]

    agent = create_tool_calling_agent(llm, tools, prompt)
    agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=False, response_format=ResponseFormat)

    # TODO async invocation for better interaction but slightly of topic and more quality of life
    return agent_executor.invoke({"input": message})


# TODO need to keep the titles of the data
def parseResult(str):
    urls = re.findall(r"\]\s*\((.*?)\)", str)
    titles = re.findall(r"\[(.*?)\]", str)

    if urls:
        return {'success': True, 'urls': urls, 'data_names': titles, 'message': 'I found your data'}
    else:
        return {'success': False, 'urls': None, 'data_names': None, 'message': str}


# Note works since we currently only use the products string needs adjustemnt if I extend API
def validateURL(urls):
    valid = []

    for url in urls:
        if "http://127.0.0.1:5000/" in url:

            parsedProduct = url.split("products/")[1].split("/")[0]

            if parsedProduct in productList:
                valid.append(url)

    if not valid:
        return ("No valid Urls found please try rephrasing your question", [])
    elif len(valid) != len(urls):
        return ("Some generated Urls are invalid please review the data and try rephrasing", valid)
    else:
        return ("I found the data you were looking for please wait while I load it ", valid)


if __name__ == "__main__":
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system",
             "You are a helpful assistant providing users with their requested data via providing opnly the link the data can be found at"),
            ("human", "{input}"),
            ("placeholder", "{agent_scratchpad}"),
        ]
    )

    retriever_tool = create_retriever_tool(
        vector_store.as_retriever(),
        "data_retriever",
        "Searches and returns Data files about the greenhouse gas emissions of diffrent countires. It  ",
    )

    tools = [retriever_tool, getDataProduct, matchFunction, extractFilter, getCatalogItem]

    agent = create_tool_calling_agent(llm, tools, prompt)
    agent_executor = create_react_agent(model=llm, tools=tools, response_format=ResponseFormat)
    agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True, response_format=ResponseFormat)

    # fdict = {"Country": "Germany", "min_year": 1990, "max_year": 1999}
    # func = "sum"
    # df = pd.read_csv('../data/data_products/' + "GHG_totals_by_country_DACH"+ '.csv')

    # applyFilter(df,fdict)
    # user = "The Co2 data for Arubas building sector where the emissions are between 0.02 and 0.03 "
    # user= "Emissions Data for Austrias Argiculture and building Sector for the 1990er"
    # user = "Sweden, Norveigen and Finnlands per capita Co2 emissions "
    user = "Sum of Germanys C02 emissions with a 5 year period"

    call = {'filename': 'GHG_totals_by_country', 'func': 'sum', 'filter': {'min_year': 1990}}
    # getDataProduct(call['filename'],call['func'],call['filter'])
    # print(extractFilter(user))
    # input = {"messages": [("user", "Sum of Germanys emission form the 1990's onward")]}
    input = {"input": user}
    agent_result = agent_executor.invoke(input)
    print(agent_result)
    # response = parseResult(agent_result['output'])
    # print(response)
