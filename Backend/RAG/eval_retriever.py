from dotenv import load_dotenv
from langchain_core.prompts import PromptTemplate

from Backend import models
from Backend.RAG.retriever import getEvaluationChain, getMultiLevelEvaluation, getEvaluationChainFunc

load_dotenv()


def productRetriever_eval(query):
    sys_prompt = """Your task is to help find the best fitting data product. You are provided with a user query.
                Provide the most likely fitting data product, always provide the data products name and why it would fit this query.
                Only provide one data product
        Context: {context} 
        The output should be a valid json with 'name': as the data product name and 'reason':
    """
    mod_query = f"I am looking for data products that can answer the query: '{query}'"
    config = {"filter": {"type": {"$eq": "product"}}}

    return getEvaluationChain(sys_prompt, config,mod_query)

def productRetriever_eval_both(query,step):
    sys_prompt = """"Your task is to help find the best fitting data product. You are provided with a user query.
                Provide the most likely fitting data product, always provide the data products name and why it would fit this query.
                Only provide one data product
        Context: {context} 
        The output should be a valid json with 'name': as the data product name and 'reason':
    """
    mod_query = f"I am looking for data products that can answer the query: '{query}', specifically the step:{step}"
    config = {"filter": {"type": {"$eq": "product"}}}

    return getMultiLevelEvaluation(sys_prompt, config,mod_query)


def functionRetriever_eval(query,type):
    """
    Searches and returns Data files aboout the functions accesible for the processing. Only usefull when trying to identify a function
    Args:
        step: a specific step in an execution plan, describing some kind of agreggation,transformation or filtering
    Returns:
        a text descrbing potential functions
    """
    sys_prompt = """Your task is to find a the top 2 fitting functions that could solve problem described in the provided step.
                Provide the most fitting function and its full description. Your answer is mewant to be short and precise exclude any additional examples.

                Context: {context} 
                Answer:
    """
    mod_query = f"I am looking for a function that can solve the following problem for me :{query}"
    config = {"filter": {"type": {"$eq": "function_NoManual2"}}}
    return getEvaluationChainFunc( config,query)

def functionRetriever_hybrid(query,type):
    """
    Searches and returns Data files aboout the functions accesible for the processing. Only usefull when trying to identify a function
    Args:
        step: a specific step in an execution plan, describing some kind of agreggation,transformation or filtering
    Returns:
        a text descrbing potential functions
    """
    sys_prompt = """Your task is to find a the top 2 fitting functions that could solve problem described in the provided step.
                Provide the most fitting function and its full description. Your answer is meant to be short and precise exclude any additional examples.

                Context: {context} 
                Answer:
    """
    mod_query = f"I am looking for a function that can solve the following problem for me :{query}"
    config = {"filter": {"type": {"$eq": "function"}}}
    return getEvaluationChainFunc( config,mod_query)

def functionRetriever_eval_noreorder(query,context):
    """
    Searches and returns Data files aboout the functions accesible for the processing. Only usefull when trying to identify a function
    Args:
        step: a specific step in an execution plan, describing some kind of agreggation,transformation or filtering
    Returns:
        a text descrbing potential functions
    """
    sys_prompt = """Your task is to find a the top 2 fitting functions that could solve problem described in the provided step.
                Provide the most fitting function and its full description. Your answer is mewant to be short and precise exclude any additional examples.

                Context: {context} 
                Answer:
    """
    mod_query = f"I am looking for a function that can solve the following problem for me :{query}"

    sys_prompt = PromptTemplate.from_template(sys_prompt).format(context=context)
    messages = [
        ("system", sys_prompt),
        ("human", mod_query),
    ]
    llm = models.get_LLM()
    return  llm.invoke(messages).content

    return getEvaluationChain(sys_prompt, config,mod_query)


def multilevelRetriever(query):
    sys_prompt = """Your task is to help find the best fitting data product. You are provided with a user query.
                Provide the most likely fitting data product, always provide the data products name and why it would fit this query.
                Only provide one data product
        Context: {context} 
        The output should be a valid json with 'name': as the data product name and 'reason': where reason should be a short explanation as to why it is the ocrrect dataset
        """
    mod_query = f"I am looking for data products that can answer the query :{query}"
    config = {"filter": {"type": {"$eq": "product"}}}
    return getMultiLevelEvaluation(mod_query,sys_prompt,config)