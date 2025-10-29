import ast
import json
import re

import requests
from dotenv import load_dotenv
from langchain_core.prompts import PromptTemplate
from ragas.llms import LangchainLLMWrapper

from Backend import models

load_dotenv()
llm = models.get_LLM()
evaluator_llm = LangchainLLMWrapper(llm)

# -------------------------------------------- generation metrics------------------------
def exact_match(generated_answer, collections):

    # all if statements account for diffrences between groundtruth and vectordb entry
    if generated_answer in ['constructors']:
        if 'constructor' in collections:
            return 1
    elif generated_answer in ['Attendance', 'Event', 'Income', 'Member', 'Budget', 'Expense', 'Major', 'Zip_Code']:
        if generated_answer.lower() in collections:
            return 1
        else:
            return 0
    elif generated_answer == 'combineProducts' and collections[0] == 'combine':
        return 1
    elif generated_answer == 'applyFilter' and collections[0] == 'filter':
        return 1
    elif generated_answer == 'retrieve' and collections[0] == 'returnResult':
        return 1
    elif generated_answer in collections:
        return 1
    else:
        return 0

def exact_match_multiple(generated_answer, ground_truth):
    # all if statements account for diffrences between groundtruth and vectordb entry
    if ground_truth == 'combine' and 'combineProducts' in generated_answer:
        return 1
    elif ground_truth == 'filter' and 'applyFilter' in generated_answer :
        return 1
    elif ground_truth == 'retrieve' and 'returnResult' in generated_answer:
        return 1
    elif ground_truth in generated_answer:
        return 1
    else:
        return 0

# -------------------------------------------- retrieval metrics------------------------
def _in_targets_product(context,targets):
    #(?:\s*|titled)\s*\"([a-z,A-Z]*)\"

    # Diffrent regex to match specific patterns
    names = re.findall(r"The dataset\s*(?:titled)*\s*\"([a-z,A-Z,_,1-9]*)\"",context)
    #names = re.findall(r"Dataset Name:\s*([a-z,A-Z,_,1-9]*)\s*(?:base_api)*", context)

    for name in names:
        if name.lower() in ['attendance', 'event', 'income', 'member','budget','expense','major','zip_code']:
            if name.lower() in targets:
                return True
        if name in targets:
            return True
    return False

def mmr(targets, contexts) -> float:
    for i in range(len(contexts)):
        if _in_targets_product(contexts[i],targets):
            return 1 / (i + 1)

    return 0


def hit_rate(relevant, contexts):

    rel = 0

    for context in contexts:
        if context in ['constructors']:
            return 1
        if _in_targets_product(context,relevant):
            return 1
    return 0

# ----------------------------------------- Functions  -------------------------------------
def _in_targets_function(context,targets):

    #Diffrent regex to match specific patterns
    names = re.findall(r"function name:\s*([a-z,A-Z]*):\n",context)
    #names = re.findall(r"function name:([a-z,A-Z]*)", context)

    for name in names:
        if name in targets:
            return True
    return False

def mmr_func(targets, contexts) -> float:

    for i in range(len(contexts)):
        if _in_targets_function(contexts[i],targets):
            return 1 / (i + 1)

    return 0

def hit_rate_func(targets, contexts) -> float:

    for i in range(len(contexts)):
        if _in_targets_function(contexts[i],targets):
            return 1

    return 0

# ----------------------------------------- other -------------------------------------

def planRecall(generated_plan, groundTruth):

    groundTruth = ast.literal_eval(groundTruth)
    generated_plan = ast.literal_eval(generated_plan)

    generated_blocks = set(_split_into_blocks(generated_plan))
    truth_blocks = set(_split_into_blocks(groundTruth))

    return len(generated_blocks & truth_blocks) / (len(generated_blocks & truth_blocks) + len(truth_blocks - generated_blocks))


def _split_into_blocks(plan):
    blocks = []
    for elem in plan["plans"]:
        if isinstance(elem, list):
            for elem_elem in elem:
                blocks.append(str(elem_elem))
        else:
            blocks.append(str(elem))

    return blocks


def jaccard(generated_plan, groundTruth):
    groundTruth = ast.literal_eval(groundTruth)
    generated_plan = ast.literal_eval(generated_plan)

    generated_blocks = set(_split_into_blocks(generated_plan))
    truth_blocks = set(_split_into_blocks(groundTruth))

    return len(generated_blocks & truth_blocks) / len(generated_blocks | truth_blocks)


def planPrecision(generated_plan, groundTruth):
    groundTruth = ast.literal_eval(groundTruth)
    generated_plan = ast.literal_eval(generated_plan)

    generated_blocks = set(_split_into_blocks(generated_plan))
    truth_blocks = set(_split_into_blocks(groundTruth))

    return len(generated_blocks & truth_blocks) / (len(generated_blocks & truth_blocks) + len(generated_blocks - truth_blocks))


def _check_block(block):
    # Test if api is valid
    response = requests.put(block["function"])
    if response.status_code == 404:
        return 0

    # Test if each value entry is valid
    func_name = block["function"].split("/")[-1]
    response = requests.get("http://127.0.0.1:5200/catalog", json={"function_name": func_name})
    func_param = json.loads(response.text)["filter_dict"][0]

    for k, v in block["values"].items():
        if k not in func_param:
            return 0

    return 1
