import re

from Backend import models
import numpy as np
from ragas.metrics import ResponseGroundedness
from ragas.metrics import Faithfulness
from ragas.metrics import SemanticSimilarity
from ragas.embeddings import LangchainEmbeddingsWrapper
from ragas.dataset_schema import SingleTurnSample
from langchain_core.prompts import PromptTemplate
from ragas.llms import LangchainLLMWrapper
from dotenv import load_dotenv
import requests
import json
import ast

load_dotenv()
llm = models.get_LLM()
evaluator_llm = LangchainLLMWrapper(llm)


def correctness_LLM_old(truth, response):
    sys_prompt = """Your Task is to decide wether a response allings with an expected output. Return a valid json with output either True if the out put aligins thematically and semantically and False if not """
    input_prompt = PromptTemplate.from_template("""expected output: {truth}\n response :{response}""")
    input_prompt = input_prompt.format(truth=truth, response=response)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]

    llm_structured = models.get_structured_LLM()
    model_out = llm_structured.invoke(messages)

    if model_out == "False":
        return 0
    elif model_out == "True":
        return 1


def correctness_LLM(truth, response):
    sys_prompt = """Your Task is to decide wether a response allings with an expected output. Return a valid json with output either True if the output describes the same Function as the given response and False otherwise"""
    input_prompt = PromptTemplate.from_template("""expected output Function: {truth}\n response :{response}""")
    input_prompt = input_prompt.format(truth=truth, response=response)
    messages = [
        ("system", sys_prompt),
        ("human", input_prompt),
    ]
    llm_structured = models.get_structured_LLM()
    model_out = llm_structured.invoke(messages)

    if (model_out["output"] == "False") or (model_out["output"] == False):
        return 0
    elif (model_out["output"] == "True") or (model_out["output"] == True):
        return 1


def simple_match(generated_answer, collections):
    score = 0
    for i in range(len(collections)):
        if collections[i] in generated_answer:
            score += 1 / len(collections)

    return score


def ragasFaithfullnes(query, response, context):
    sample = SingleTurnSample(
        user_input=query,
        response=response,
        retrieved_contexts=context
    )
    scorer = Faithfulness(llm=evaluator_llm)
    # res = scorer.single_turn_score(sample)
    # print(res)
    return scorer.single_turn_score(sample)


def ragasResponseGroundedness(response, context):
    sample = SingleTurnSample(
        response=response,
        retrieved_contexts=context
    )

    scorer = ResponseGroundedness(llm=evaluator_llm)
    return scorer.single_turn_score(sample)


def ragasSemanticSimilarity(response, truth):
    sample = SingleTurnSample(
        response=response,
        reference=truth
    )

    scorer = SemanticSimilarity(embeddings=LangchainEmbeddingsWrapper(models.get_embeddings()))
    return scorer.single_turn_score(sample)


# -------------------------------------------- retrieval metrics------------------------
def _in_targets_product(context,targets):

    names = re.findall(r"The dataset titled\s*\"([a-z,A-Z]*)\"",context)

    for name in names:
        if name in targets:
            return True
    return False

def mmr(targets, contexts) -> float:
    for i in range(len(contexts)):
        if _in_targets_product(contexts[i],targets):
            return 1 / (i + 1)

    return 0


def precison(relevant, contexts):
    rel = 0
    for context in contexts:
        if _in_targets_product(context,relevant):
            rel += 1

    return rel / len(contexts)


def recall(relevant, contexts):
    rel = 0
    for context in contexts:
        if _in_targets_product(context,relevant):
            rel += 1

    return rel / len(relevant)


def F1(precision, recall):
    score = []
    for p, r in zip(precision, recall):
        try:
            score.append(2 * ((p * r) / (p + r)))
        except:
            score.append(0)
    return score


# ----------------------------------------- stuff -------------------------------------
def _in_targets_function(context,targets):

    names = re.findall(r"function name:\s*([a-z,A-Z]*):\n",context)

    for name in names:
        if name in targets:
            return True
    return False

def mmr_func(targets, contexts) -> float:

    for i in range(len(contexts)):
        if _in_targets_function(contexts[i],targets):
            return 1 / (i + 1)

    return 0


def precison_func(relevant, contexts):
    rel = 0
    for context in contexts:
        if _in_targets_function(context,relevant):
            rel += 1

    return rel / len(contexts)


def recall_func(relevant, contexts):
    rel = 0
    for context in contexts:
        if _in_targets_function(context,relevant):
            rel += 1

    return rel / len(relevant)

def hit_rate_func(targets, contexts) -> float:

    for i in range(len(contexts)):
        if _in_targets_function(contexts[i],targets):
            return 1

    return 0
# ----------------------------------------- stuff -------------------------------------

def planRecall(generated_plan, groundTruth):
    generated_blocks = _split_into_blocks(generated_plan)
    truth_blocks = _split_into_blocks(ast.literal_eval(groundTruth))
    return recall(generated_blocks, truth_blocks)


def _split_into_blocks(plan):
    blocks = []
    for elem in plan["plans"]:
        if isinstance(elem, list):
            for elem_elem in elem:
                blocks.append(str(elem_elem))
        else:
            blocks.append(str(elem))

    for elem in plan["combination"]:
        if isinstance(elem, list):
            for elem_elem in elem:
                blocks.append(str(elem_elem))
        else:
            blocks.append(str(elem))

    return blocks


def jaccard(generated_plan, groundTruth):
    generated_blocks = set(_split_into_blocks(generated_plan))
    truth_blocks = set(_split_into_blocks(ast.literal_eval(groundTruth)))

    return len(generated_blocks & truth_blocks) / len(generated_blocks | truth_blocks)


def planPrecision(generated_plan, groundTruth):
    generated_blocks = _split_into_blocks(generated_plan)
    truth_blocks = _split_into_blocks(ast.literal_eval(groundTruth))
    return precison(generated_blocks, truth_blocks)


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


def schemaAdherence(generated_plan):
    """Either one or zero ?"""
    generated_blocks = _split_into_blocks(generated_plan)
    res = 0
    l = 0
    for block in generated_blocks:
        block = ast.literal_eval(block)
        if "function" in block:
            l += 1
            res += _check_block(block)

    return res / l
