import ast
import os

import pandas as pd
import yaml
import requests
import json

from dotenv import load_dotenv
from langchain_postgres.vectorstores import PGVector
from langchain.agents import AgentExecutor, create_tool_calling_agent, tool
from langchain_openai import AzureOpenAIEmbeddings, AzureChatOpenAI
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate
from langchain_core.callbacks import UsageMetadataCallbackHandler
from langchain_community.callbacks import get_openai_callback
from Backend import models
from Backend.Agent import execute
from Backend.RAG import vector_db,retriever

def reiterate_plan(steps, query):
    """
        Breaks down a user query into its multiple steps
        Args:
            query:  a natural languge query
            steps: the first ioteration of steps
            collection_name: the name of the collection needed to solve
        Returns:
            a list of corrected steps necerssary to solve the query
        """

    # Identify fitting data collection
    prod_descriptions = retriever.collection_rag(query, config=None)

    # Retrieve corresponding data catalog
    response = requests.get("http://127.0.0.1:5000/catalog/collection",
                            json={"file": prod_descriptions["collection_name"]})

    catalog = json.loads(response.text)
    #print(catalog)
    sys_prompt = """ Your task is to decide wheather a plan is executable or not, and if it is not executable how to fix the plan
        For this consider if all necerssary columns are in the retrieved data product, if the column selection makes sense etc.
        Ignore computations such as mean of or sum
        You can remove steps from the list, but this is a last resort and steps should always be updated 
        
        Do not list steps such as ensure, only list active steps in the instructions
        The output should be a valid json with "decision": either True or False, and "instructions" a list of steps needed to make the plan executable
        
        In the instructions do only list steps that acitvely change the plan

        """

    input_prompt = PromptTemplate.from_template("""
                User Query:{query}
                product catalog :{catalog}
                steps:{steps}
                """)
    input_prompt = input_prompt.format(query=query, catalog=catalog, steps=steps)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    llm = models.get_structured_LLM()
    response = llm.invoke(messages)

    return response

def correct_plan(plan, instructions):
    sys_prompt = """ You are given a plan and a number of instructiuons on how to fix this plan 
            keep to the schema of the original plan, while applying the instructions
            The output should be a valid json with "plan": as the corrected plan
            """

    input_prompt = PromptTemplate.from_template("""
                    Plan:{plan}
                    instructions: {instructions}
                    """)
    input_prompt = input_prompt.format(plan=plan, instructions=instructions)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    llm = models.get_structured_LLM()
    response = llm.invoke(messages)

    return response

def manual_correction(agent_result):
    plan = agent_result["plans"]

    def helper(instance):
        if isinstance(instance, (int, float, complex, str)) and not isinstance(instance, bool):
            return instance
        elif isinstance(instance,list):
            for i in range(len(instance)):
                if not isinstance(instance[i], (int, float, complex, str)):
                    instance[i] = str(instance[i])
            return instance
        else:
            return str(instance)

    for i in range(len(plan)):
        params = plan[i]['filter_dict']
        for k, v in params.items():
            if isinstance(v, dict):
                for kk, vv in v.items():
                    if isinstance(vv, dict):
                        for kkk, vvv in vv.items():
                            params[k][kk][kkk] = helper(vvv)

                    else:
                        params[k][kk]= helper(vv)
            else:
                params[k] = helper(v)

    agent_result["plans"] = plan

    return agent_result

def critic(call, product_api):
    product_name = product_api.split("/")[-1]
    response = requests.get("http://127.0.0.1:5000/catalog/columns", json={"file": product_name})
    columns = json.loads(response.text)
    function_name = call["function"].split("/")[-1]
    response = requests.get("http://127.0.0.1:5200/catalog", json={"function_name": function_name})
    function = json.loads(response.text)



    sys_prompt = """ Your task is to verrify and correct a api call if necersary, you are presented with an api call and the information used to generate that call.
                Verify that all calls are correct, that means only parameters present in the function specification should be present in the corrected version as well as only coolumn names present in the product specification should be present 
                The output should be a valid json concisting of 
                "correction_needed": either True or False, True if something needed to be corrected Flase if not
                "call": either the corrected function call or the orginal one if no corrections where needed
                "explanation": an explanantion of the corrections

                Example:
                call: {{"function":"http://127.0.0.1:5200/filter","filter_dict":{{"columns":"categories","criteria":{{"category":"book"}} }} }}


                Function specification:
                filter:
                base_api: http://127.0.0.1:5200/filter
                description: "filters a data product based on the provided filterdict, it filters row wise keeping all input columns"
                filter_dict:
                - conditions: "a dict ith the corresponding columns and their values"
                example: "{'condition':{'location':'berlin','year':{'min':2002,'max':2007}}"


                Product specification:
                [invoice_no,customer_id,category,quantity,price,invoice_date,shopping_mall]

                result:
                correction_needed: True, call: {{"function":"http://127.0.0.1:5200/filter","filter_dict":{{"conditions":{{"category":"book"}} }} }}, 'explanation': criteria in the filter_dict is not valid according to the function specifications, and categories does not appear in the column list, category does
        """

    input_prompt = PromptTemplate.from_template("""
                Original Call: {call}
                Function specification: {function}
                Product specification:{columns}
                """)
    input_prompt = input_prompt.format(call=call, function=function, columns=columns)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    # llm = models.get_LLM()
    llm = models.get_structured_LLM()
    response = llm.invoke(messages)
    return response["call"]


def critique_plan(agent_result):
    plan = agent_result["plans"]

    for i in range(len(plan)):
        print(i)
        elem = plan[i]
        if elem['function'] == 'http://127.0.0.1:5200/retrieve':
            product = elem["filter_dict"]["product"]
        if elem['function'] == 'combination':
            continue
        plan[i] = critic(elem, product)

    agent_result["plans"] = plan
    return agent_result

def critique_plan_df(agent_result):
    try:
        agent_result = ast.literal_eval(agent_result)
        plan = agent_result["plans"]

        for i in range(len(plan)):
            elem = plan[i]
            if elem['function'] == 'http://127.0.0.1:5200/retrieve':
                product = elem["filter_dict"]["product"]
            if elem['function'] == 'combination':
                continue
            plan[i] = critic(elem, product)

        agent_result["plans"] = plan
    except:
        pass

    return agent_result

def critique_plan_df2(steps, query,evidence):
    mod_query = f"The query i want to solve: {query}, some additional information: {evidence}"
    #response = reiterate_plan(steps, mod_query)

    #if not response["decision"]:
    #    steps = correct_plan(steps,response["instructions"])["plan"]


    try:
        steps = ast.literal_eval(steps)
        steps = manual_correction(steps)
    except:
        pass

    return steps

def correct_run2(file):
    df = pd.read_csv(file)
    res = []

    for index, row in df.iterrows():
        tmp = critique_plan_df2(row["response"],row["query"],row["evidence"])
        res.append(tmp)

    df["response"] = res
    df.to_csv(f"{file}_cirtiqued",index=False)

def correct_run(file):
    df = pd.read_csv(file)

    df = df[df["question_id"] == 1505]
    df["response"] = df["response"].apply(lambda x:critique_plan_df2(x))

    df.to_csv(f"{file}_cirtiqued",index=False)

if __name__ == "__main__":
    file = "../evaluation/prototype_eval_column_info_2025-08-12-22-24_cirtiqued.csv"
    correct_run2(file)

    test = {'plans':
[{'function': 'http://127.0.0.1:5200/retrieve', 'filter_dict': {'product': 'http://127.0.0.1:5000/products/debit_card_specializing/customers'}},
 {'function': 'http://127.0.0.1:5200/filter', 'filter_dict': {'conditions': {'Currency': 'EUR'}}},
 {'function': 'http://127.0.0.1:5200/retrieve', 'filter_dict': {'product': 'http://127.0.0.1:5000/products/debit_card_specializing/yearmonth'}},
 {'function': 'http://127.0.0.1:5200/filter', 'filter_dict': {'conditions': {'Consumption': {'min': 1000}}}},
 {'function': 'combination', 'filter_dict': {'columns_left': 'CustomerID', 'columns_right': 'CustomerID', 'type': 'equals', 'values': ['None']}},
 {'function': 'http://127.0.0.1:5200/count', 'filter_dict': {'columns': 'customer_id', 'unique': True}}]}
    #print(critique_plan(test))

    sql = "Among the customers who paid in euro, how many of them have a monthly consumption of over 1000?"
    ev = "Pays in euro = Currency = 'EUR'."

    query = f"The query i want to solve: {sql},some additional information:{ev}"
    inst = [
{'step': 5, 'update': {'function': 'combination', 'filter_dict': {'columns_left': 'CustomerID', 'columns_right': 'CustomerID', 'type': 'equals'}}},
{'step': 6, 'update': {'filter_dict': {'columns': 'CustomerID', 'unique': True}}}]
    print(manual_correction(test))
    #print(correct_plan(test,inst))
    #print(reiterate_plan(test, query))