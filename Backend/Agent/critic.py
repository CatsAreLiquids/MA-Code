import ast
import json

import pandas as pd
import requests
from langchain_core.prompts import PromptTemplate

from Backend import models
from Backend.RAG import retriever


def reiterate_plan(steps, query):

    # Identify fitting data collection
    prod_descriptions = retriever.collection_rag(query, config=None)

    # Retrieve corresponding data catalog
    response = requests.get("http://127.0.0.1:5000/catalog/collection",
                            json={"file": prod_descriptions["collection_name"]})
    try:
        catalog = json.loads(response.text)
    except:
        catalog = ""

    sys_prompt = """ Your task is to decide wheather a plan is executable or not, and if it is not executable how to fix the plan
        For this consider if all necerssary columns are in the retrieved data product, if the column selection makes sense etc.
        Ignore computations such as mean of or sum, and the value paramter
        
        You can remove steps from the list, but this is a last resort and steps should always be updated 
        
        If a filter step makes no sense because the columns are not present cosider updating the  retrieval step
        
        count and mean need an existing column as basis
        
        When updating state explicitly what should be changed
        
        combination steps are a valid part of the plan as they join tables together, providing only the column name is viable
        ensure that enough combinations are present, so that all products are combined
        You can add combinations steps an example: {{"function":"combination","filter_dict":{{"columns_left":"column_name","columns_right":"column_name","type":"equals","values":["None"]}} }}
        For any combination step you add make sure the fitting data product is present as well
        
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

def critique_plan(steps, query):
    num_iterations = 0

    while num_iterations < 3 :
        response = reiterate_plan(steps, query)
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

def correct_run(file, evidence=True):
    df = pd.read_csv(file)
    res = []

    for index, row in df.iterrows():
        if evidence:
            modded_query = f"The query i want to solve: {row['query']}, some additional information: {row['evidence']}"
        else:
            modded_query = f"The query i want to solve: {row['query']}"

        tmp = critique_plan(row["response"],modded_query)
        res.append(tmp)

    df["response"] = res
    df.to_csv(f"{file}_cirtiqued_single",index=False)


if __name__ == "__main__":
    """
    To correct a full run use correct_run, alternativley the reiterate_plan and correct_plan can be used to correct a singluar plan
    """
    pass