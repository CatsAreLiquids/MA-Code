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
import RAG
from langchain_core.prompts import PromptTemplate
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate
from langchain.tools.retriever import create_retriever_tool
from datetime import date
import requests


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
def extendFilter(keywords):
    """
    provides additional keywords based on the input list,
    :return: a comma seperated list of keywords: [keyword1,keyword2,keyword3,...]
    """

    # remove all numerical keywords first
    keywords= [keyword for keyword in keywords if not isinstance(keyword, int)]
    print(keywords)
    # Note based on langchain multiquery prompt
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
                    You are an AI language model assistant. Your task is to generate alternative keywords for each keyword in the given list.
                    Generating these keywords will help with filtering data annotated by multiple sources.
                    The goal is it to provide extended filter keywords after we were unsuccsesfull with the first keywords.
                    Therefore we need to boarden the scope of the keywords, in the sense that "Europe" is an alternative keyword for "Ireland"
                    Provide the alternative keywords as a comma seperated list: ["keyword1","keyword2","keyword3"].
                """
            ),
            ("user", keywords),
        ]
    )

    llm = AzureChatOpenAI(
        azure_endpoint=os.environ["GPT_EndPoint"],
        openai_api_version=os.environ["GPT_APIversion"],
    )

    response = llm.invoke(prompt.format(input=keywords))
    return response.content

@tool
def createFilter(query,keywords):
    """
        creates a filter for a retriever based on the provided user query and keyweords
        :return: a list of filters [filter={'tags': {'$ilike': '%Germany%'},...]

    """
    sys_prompt = """You are an AI language model assistant. Your task is to generate a string filter  based on a user query and a list of given keywords.
                With the example : 
                user query : "I would like to have the generall CO2 emissions data of Germany for the years 2000 to 2010"
                keywords: ["CO2 emissions", "Germany", 2000, 2010]
                The result should look like:
                [filter={'tags': {'$ilike': '%Germany%'},{'tags': {'$ilike': '%CO2 emissions%'}], 'min_year': {'$lte': 2000},'max_year': {'$gte': 2010} }}
                Only use the following filters:
                Text (case-insensitive like):'$ilike'
                Logical (or):'$or',
                Logical (and):'$and',
                Greater than (>):'$gt',
                Greater than or equal (>=):'$gte',
                Less than (<):'$lt',
                Less than or equal (<=):'$lte'
                """
    input_prompt= PromptTemplate.from_template("""
    User Query:{query}
                keywords: {keywords}
    """)
    input_prompt = input_prompt.format(query=query, keywords=keywords)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages)

@tool
def getDataProduct(filename):
    """
    rettrieves the a dataproduct based on a filename
    :return: a json of the data product
    """
    url = "http://127.0.0.1:5000/products?file="#GHG_totals_by_country_DACH
    r = requests.get(utl+filename)
    data = r.json()
    return data['message']


#TODO
def validateKeywords():

    return

#TODO streamm all the tool calls not just the result
#TODO should this be a graph ? could be helpfull for keyword validation etc
#TODO disambiguation Germany and EU27 how to do this
if __name__ == "__main__":
    load_dotenv()

    embeddings = AzureOpenAIEmbeddings(
        model="text-embedding-ada-002",
        azure_endpoint=os.getenv("TextEmb_EndPoint")
    )
    llm = AzureChatOpenAI(
        azure_endpoint=os.environ["GPT_EndPoint"],
        openai_api_version=os.environ["GPT_APIversion"],
    )

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", "You are a helpful assistant"),
            ("human", "{input}"),
            # Placeholders fill up a **list** of messages
            ("placeholder", "{agent_scratchpad}"),
        ]
    )

    connection = "postgresql+psycopg://langchain:langchain@localhost:6024/langchain"  # Uses psycopg3!
    collection_name = "my_docs"

    vector_store = PGVector(
        embeddings=embeddings,
        collection_name=collection_name,
        connection=connection,
        use_jsonb=True,
    )

    retriever_tool = create_retriever_tool(
        vector_store.as_retriever(),
        "data_retriever",
        "Searches and returns Data files about the greenhouse gas emissions of diffrent countires.",
    )

    tools = [extractKeywords,extendFilter,createFilter,retriever_tool]

    agent = create_tool_calling_agent(llm, tools, prompt)
    agent_executor = AgentExecutor(agent=agent, tools=tools,verbose=True)


    user = "Can you show me the Co2 output per GDP for Germany"
    response = agent_executor.invoke({"input": user})
    print(response)


