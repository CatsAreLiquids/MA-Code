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

def correct_run(file):
    df = pd.read_csv(file)

    df["response"] = df["response"].apply(lambda x:critique_plan_df(x))

    df.to_csv(f"{file}_cirtiqued",index=False)
if __name__ == "__main__":
    file = "../evaluation/prototype_eval_2025-07-31-17-23.csv"
    correct_run(file)