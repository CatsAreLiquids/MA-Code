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
    print(catalog)
    sys_prompt = """ Your task is to decide wheather a plan is executable or not, and if it is not executable how to fix the plan
        For this consider if all necerssary columns are in the retrieved data product, if the column selection makes sense etc.
        Ignore computations such as mean of or sum, and the value paramter
        
        You can remove steps from the list, but this is a last resort and steps should always be updated 
        
        If a filter step makes no sense because the columns are not present cosider updating the  retrieval step
        
        When updating state explicitly what should be changed
        
        combination steps are a valid part of the plan as they join tables together, providing only the column name is viable
        ensure that enough combinations are present, so that all products are combined
        You can add combinations steps
        
        Steps that filter for an unklnown specific value or references previous results, are not feasible, remove them
        
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
            Combinations steps should immediatly be after the relevant data products have been processed
            
            Do not include duplicate steps, especially combinations
            Combinations steps should immediatly be after the relevant data products have been processed
            combination steps look like: {{"function":"combination","filter_dict":{{"columns_left":"customer_id","columns_right":"customer_id","type":"equals","values":["None"]}} }}
            
            DO not include columns in the retrieval 
            
            The output should be a valid json with "plans": as the corrected plan
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
    num_iterations = 0

    while num_iterations <= 3:
        response = reiterate_plan(steps, mod_query)

        if not response["decision"]:
            tmp = correct_plan(steps,response["instructions"])
            if isinstance(list(tmp.values())[0], list):
                steps = tmp
            elif isinstance(list(tmp.values())[0], dict):
                steps = correct_plan(steps,response["instructions"])["plans"]
            num_iterations+= 1

        else:
            break

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
    df.to_csv(f"{file}_cirtiqued_single",index=False)

def correct_run(file):
    df = pd.read_csv(file)

    df = df[df["question_id"] == 1505]
    df["response"] = df["response"].apply(lambda x:critique_plan_df2(x))

    df.to_csv(f"{file}_cirtiqued",index=False)

if __name__ == "__main__":
    file = "../evaluation/prototype_eval_column_info_2025-08-12-22-24_cirtiqued.csv"
    correct_run2(file)

    test = {'plans': [{'function': 'http://127.0.0.1:5200/retrieve', 'filter_dict': {'product': 'http://127.0.0.1:5000/products/formula_1/races'}},
 {'function': 'http://127.0.0.1:5200/filter', 'filter_dict': {'conditions': {'raceId': 901}}},
 {'function': 'http://127.0.0.1:5200/retrieve', 'filter_dict': {'product': 'http://127.0.0.1:5000/products/formula_1/seasons'}}]}

    #print(critique_plan(test))

    sql = "Show me the season page of year when the race No. 901 took place."
    ev = "the season page refers to url; race number refers to raceId;"

    query = f"The query i want to solve: {sql},some additional information:{ev}"
    inst = [
{'step': "Update the retrieval step for 'races' to include the 'year' column, as it is necessary to determine the year of the race."},
{'step': "Add a combination step to join the 'races' and 'seasons' products using the 'year' column, as this is required to link the race year to the season URL."}]
    #print(manual_correction(test))
    #print(correct_plan(test,inst))
    #print(reiterate_plan(test, query))