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


from Agent.tools import init_tools

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
    print(first,second)
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
        print("first pre:", df)
        df = _executeBlocks(df, plan['combine']['p1'][1])
        print("first post:", df)
        df2 = _getDataProduct(plan['combine']['p2'][0])

        df2 = _executeBlocks(df2, plan['combine']['p2'][1])
        df = _combineProducts(df, df2, plan['column'], plan['type'], plan['values'])

    else:
        df = _getDataProduct(plan['execute']['p1'][0])
        df = _executeBlocks(df, plan['execute']['p1'][1])

    return df


def _executeBlocks(df, plan):
    try:
        if isinstance(plan,str):
            plan = ast.literal_eval(plan)
        for elem in plan:
            if 'values' in elem:
                values = elem['values']
            else:
                values = None
            df = applyFunction(df, elem['function'], values)
    except:
        print("error")

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
                If more than one data product is needed use the combine structure otherwise use execute.
                
                The result should be a valid json
            
                Examples for the result are:
                user query: "All females customers who paid with Credit Card and are at least 38 years old"
                result: {{"execute":{{"p1":({{"name": "sales_data_23", "url": "http://127.0.0.1:5000/products/Sales_Data/sales_data_23"}},[{{"function":"filter","values":{{"gender":"Female","age":{{"min":38}} }} }},{{"function":"getRows","values":{{"customer_id":"None"}} }}])}} }}
                user query: "Total cost per category bought by women over 38 who paid with credit card"
                result: {{"combine":{{"p1":({{"name": "sales_data_23", "url": "http://127.0.0.1:5000/products/Sales_Data/sales_data_23"}},[{{"function":"sum","values":{{"group_by":"category","column":"price"}} }} ]),"p2":({{"name": "customer_data_23", "url": "http://127.0.0.1:5000/products/Sales_Data/customer_data_23"}},[{{"function":"filter","values":{{"gender":"Female","age":{{"min":38}} }} }} , {{"function":"getRows","values":{{"customer_id":"None"}} }}] )}},"column":"customer_id","type":"select","values":["None"]}}
                
                Do only return the result and do not explain it 
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

#tools = [breakDownQuery]
tools = init_tools()
agent = create_tool_calling_agent(llm, tools, prompt)

planning_agent = AgentExecutor(agent=agent, tools=tools, verbose=True)
agent_result= planning_agent.invoke({'input':user})['output']
#agent_result = ast.literal_eval(agent_result)
#print(agent_result)
l = {"combine":{"p1":({"name": "sales_data_23", "url": "http://127.0.0.1:5000/products/Sales_Data/sales_data_23"},[{"function":"sum","values":{"operation":"sum","field":"amount_spent"}}]),"p2":({"name": "customer_data_23", "url": "http://127.0.0.1:5000/products/Sales_Data/customer_data_23"},[{"function":"filter","values":{"gender":"Female","age":{"min":38}}},{"function":"getRows","values":{"customer_id":"None"}}])},"column":"customer_id","type":"select","values":["None"]}

#print(execute(agent_result))
r1 = "sales_data_23"
p2 = [{'function': 'filter', 'values': {'gender': 'Female', 'age': {'min': 38}, 'payment': 'Credit Card'}},
      {'function': 'getRows', 'values': {'customer_id': 'None'}}]


# alt = {'execute':{'p1':(r1,one)}}
# plan = {'execute':{'p1':{'product':{'name': 'sales_data_23', 'url': 'http://127.0.0.1:5000/products/Sales_Data/sales_data_23'},[{"function":"filter","values":{"gender":"Female","age":{"min":38},"payment":"Credit Card"}},{"function":"getRows","values":{"customer_id":"None"}}])}}
# print(execute(plan))
# print(execute(alt))
