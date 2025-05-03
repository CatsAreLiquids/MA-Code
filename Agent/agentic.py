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
def formatOutput():
    """

    :return:
    """
    pass

@tool
def chooseProduct(query,retrievedProducts):
    """
    Decides the best possible data product based on a user query and the retrieved products
    :param query: text about the queries
    :param retrievedProducts: data product decription and data product name
    :return: {{"name":"name","url":"http://127.0.0.1:5000/exampleUrl"}}
    """
    sys_prompt = """ Your task is to help identify the correct url and data product for a user based on their query
                        Only provide one url at a time together withe the name of the data product.
                        The output should be a valid json formatted as follwos:
                        {{"name":"name","url":"http://127.0.0.1:5000/exampleUrl"}}
        """
    input_prompt = PromptTemplate.from_template("""
            User Query:{query}
            possible Products:{retrievedProducts}
            """)
    input_prompt = input_prompt.format(query=query, catalog_dict=retrievedProducts)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    return llm.invoke(messages)

@tool
def breakDownQuery(query):
    """
    Breaks down a user query into multiple steps
    :param query: user query received at the start
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
    dataProduct: name of the data product ( by identifyDataProduct )
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


def _combineProducts(first, second, column, type, value):
    if type == "select":
        first = first[first[column].isin(value)]
    if type == "join":
        first = first.join(second, on=column)
        if value is not None:
            first = first[first[column].isin(value)]
    return first


def execute(plan):
    if 'combine' in plan:
        df = _getDataProduct(plan['combine']['p1'][0])
        df = _executeBlocks(df, plan['combine']['p1'][1])

        df2 = _getDataProduct(plan['combine']['p2'][0])
        df2 = _executeBlocks(df2, plan['combine']['p2'][1])

        df = _combineProducts(df, df2, plan['column'], plan['type'], plan['values'])

    else:
        df = _getDataProduct(plan['execute']['p1'][0])
        df = _executeBlocks(df, plan['execute']['p1'][1])

    return df


def _executeBlocks(df, plan):
    try:
        plan = ast.literal_eval(plan)
        for elem in plan:
            if 'values' in elem:
                values = elem['values']
            else:
                values = None
            df = applyFunction(df, elem['function'], values)
    except:
        pass

    return df


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
user = "From the sales data i would like to know the total amount of money spent per category of available items, of women over 38"
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

sys_prompt = """ Your task is to create an execution plan for a user query.
                To do this you first need to break the user query in possible multiple parts and the iodentfy data products and how to process these products
                For this combine the results of the createExecutionPlan and identifyDataProduct tool to create find each necerssary data product as well as a execution plan for it.
                If more than one data product is needed use the combine structure otherwise use execute.
                
                The result should be a valid json
            
                Examples for the result are:
                user query: "All females customers who paid with Credit Card and are at least 38 years old"
                result: {{"execute":{{"p1":({{"name": "sales_data_23", "url": "http://127.0.0.1:5000/products/Sales_Data/sales_data_23"}},[{{"function":"filter","values":{{"gender":"Female","age":{{"min":38}} }} }},{{"function":"getRows","values":{{"customer_id":"None"}} }}])}} }}
                user query: "Total cost per category bought by women over 38 who paid with credit card"
                result: {{"combine":{{"p1":({{"name": "sales_data_23", "url": "http://127.0.0.1:5000/products/Sales_Data/sales_data_23"}},[{{"function":"sum","values":{{"group_by":"category"}} }} ]),"p2":({{"name": "customer_data_23", "url": "http://127.0.0.1:5000/products/Sales_Data/customer_data_23"}},[{{"function":"filter","values":{{"gender":"Female","age":{{"min":38}} }} }} , {{"function":"getRows","values":{{"customer_id":"None"}} }}] )}},'column':"customer_id",'type':'select','values':["C109593"]}}
                
                Do only return the result and do not explain it 
                
                
    """
prompt = ChatPromptTemplate.from_messages(
    [
        ("system", sys_prompt),
        ("human", "input"),
        ("placeholder", "{agent_scratchpad}"),
    ]
)

retriever_tool = create_retriever_tool(
        vector_store.as_retriever(),
        "data_retriever",
        "Searches and returns Data files aboout diffrent statistics ",
    )

tools = [identifyDataProduct,createExecutionPlan,breakDownQuery]

agent = create_tool_calling_agent(llm, tools, prompt)

planning_agent = AgentExecutor(agent=agent, tools=tools, verbose=True)

agent_result = planning_agent.invoke({"input": user})['output']
print(agent_result)
agent_result = ast.literal_eval(agent_result)
#print(breakDownQuery.invoke(user))
print(execute(agent_result))
r1 = "sales_data_23"
p2 = [{'function': 'filter', 'values': {'gender': 'Female', 'age': {'min': 38}, 'payment': 'Credit Card'}},
      {'function': 'getRows', 'values': {'customer_id': 'None'}}]

#print(verfiyPlan(r1, p2))

# alt = {'execute':{'p1':(r1,one)}}
# plan = {'execute':{'p1':{'product':{'name': 'sales_data_23', 'url': 'http://127.0.0.1:5000/products/Sales_Data/sales_data_23'},[{"function":"filter","values":{"gender":"Female","age":{"min":38},"payment":"Credit Card"}},{"function":"getRows","values":{"customer_id":"None"}}])}}
# print(execute(plan))
# print(execute(alt))
