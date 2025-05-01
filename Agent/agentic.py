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
filters = {'getRows': filter.getRows,'filter':filter.applyFilter,'combine':filter.combineProducts}



@tool
def identifyDataProduct(query):
    """
    Calls a retriever agent that identfies the most fitting data product for the input query
    query: a user query defining a specifc data product
    :return: {{"name":"name","url":"http://127.0.0.1:5000/exampleUrl"}}
    """
    ragent = retrieval_agent.init_agent()
    agent_result = ragent.invoke({"input": query})['output']
    agent_result = json.loads(agent_result)

    return agent_result


@tool
def createExecutionPlan(query, dataProduct):
    """
    Calls an agent system that creates an execution plan based on defined functions and the user query
    query: a user query defining a specifc data product
    dataProduct: name of the data product (retrieved with identifyDataProduct )
    :return: [{{"function":"filter","values":{{"gender":"Female","age":{{"min":38,"max":38}} }} }},{{"function":"getRows","values":{{"customer_id":"None"}} }}]
    """
    query = query + f"The correct data products name is {dataProduct}"
    pagent = processing_agent.init_agent()
    agent_result = pagent.invoke({"input": query})['output']
    agent_result = ast.literal_eval(agent_result)

    return agent_result

def _getDataProduct(agent_result):
    """
    :param agent_result:
    :return:
    """
    try:
        return util.getData(agent_result['url'])
    except:
        return "could not access data product is the URL correct ?"

def executeQuery():
    pass

def tmp(exc_pattern,url):
    """
    :param exc_pattern:
    :param url:
    :return:
    """
    df = _getDataProduct()
    try:
        agent_result = ast.literal_eval(agent_result)
        for elem in agent_result:
            if 'values' in elem:
                values = elem['values']
            else:
                values = None
            df = applyFunction(df, elem['function'], values)
    except:
        pass

def applyFunction(df, function, values):
    if function in aggregations:
        return aggregations[function](df, values)
    if function in filters:
        return filters[function](df, values)

    return df


# user = "The Co2 data for Arubas building sector where the emissions are between 0.02 and 0.03 "
# user= "Emissions Data for Austrias Argiculture and building Sector for the 1990er"
# user = "Sweden, Norveigen and Finnlands per capita Co2 emissions "
user = "All females customers who paid with Credit Card and are at least 38 years old"
user = "Total cost per category bought by women over 38 who paid with credit card"
# user= "Germanies emisson for the 2000s"

"""
ragent = retrieval_agent.init_agent()
pagent = processing_agent.init_agent()

agent_result = ragent.invoke({"input": user})['output']
agent_result = json.loads(agent_result)
if ('name' in agent_result) and ('url' in agent_result):
    df = util.getData(agent_result['url'])
    user = user + f"The correct data products name is {agent_result['name']}"
agent_result = pagent.invoke({"input": user})['output']
try:
    agent_result = ast.literal_eval(agent_result)
    for elem in agent_result:
        if 'values' in elem:
            values = elem['values']
        else:
            values = None
        df = applyFunction(df, elem['function'], values)
except:
    pass
"""

# JSONDecodeError

sys_prompt = """ Your task is to identify the seperate steps and data products necerssary to answer a user query. 
                To complete this goal break every user query into multiple steps if multiple data products are necerssary.
                you have access to 

    """
prompt = ChatPromptTemplate.from_messages(
    [
        ("system", sys_prompt),
        ("human", "{input}"),
        ("placeholder", "{agent_scratchpad}"),
    ]
)

tools = [identifyDataProduct,createExecutionPlan]

agent = create_tool_calling_agent(llm, tools, prompt)

planning_agent = AgentExecutor(agent=agent,tools=tools, verbose=True)

agent_result = planning_agent.invoke({"input": user})['output']
print(agent_result)