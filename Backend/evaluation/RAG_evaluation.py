from Backend.RAG import vector_db
from Backend import models
from Backend.evaluation import metrics
import pandas as pd
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

def productRetriever(query):
    sys_prompt = """Your task is to help find the best fitting data product. You are provided with a user query .
                Provide the most likely fitting data products, always provide the data products name and why it would fit this query
        Context: {context} 
        Answer:
    """
    mod_query = f"I am looking for data products that can answer the query: '{query}'"
    config = {"filter": {"type": {"$eq": "product"}}}

    return vector_db.getEvaluationChain(sys_prompt, config,mod_query)


def functionRetriever(query,type):
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
    config = {"filter": {"type": {"$eq": type}}}
    return vector_db.getEvaluationChain(sys_prompt, config,mod_query)

def db_pediaRetriever(query):
    """
    Searches and returns Data files aboout the functions accesible for the processing. Only usefull when trying to identify a function
    Args:
        step: a specific step in an execution plan, describing some kind of agreggation,transformation or filtering
    Returns:
        a text descrbing potential functions
    """
    sys_prompt = """Your task is to help find the best fitting data entry. You are provided with a user query .
                Provide the most likely fitting data entry, always provide the data entrys name and why it would fit this query
        Context: {context} 
        Answer:
    """
    mod_query = f"I am looking for a data entry that can solve the following problem for me :{query}"
    config = None
    return vector_db.getEvaluationChain(sys_prompt, config,mod_query,collection="DB_PEDIA")


def run_test(file):
    df = pd.read_csv(file,converters={'retrieved_docs': pd.eval,'names': pd.eval,"correct_context":pd.eval})
    functions = {"product": productRetriever, "function": functionRetriever,"function_text":functionRetriever, "db_pedia": db_pediaRetriever,}

    answer, context = [],[]
    for index, row in tqdm(df.iterrows()):
        if 'skip' in df.columns:
            skip = row["skip"]
        else:
            skip = False
        if not skip:
            if "function" in row["type"]:
                res = functions[row["type"]](row["query"],row["type"])
            else:
                res = functions[row["type"]](row["query"])
            answer.append(res["response"])
            context.append(res["docs"])
            print(res["docs"])
        else:
            answer.append([""])
            context.append([""])

    df["response"] = answer
    df["retrieved_docs"] = context

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    df.to_csv(f"runs/{Path(Path(file).name).stem}_{timestamp}.csv",index=False,encoding='utf8')

def run_score(file):
    #df = pd.read_csv(file)
    #print(df.head())
    df = pd.read_csv(file,converters={'retrieved_docs': pd.eval,'names': pd.eval,"correct_context":pd.eval},encoding='utf8')
    mmr = []
    precision = []
    recall = []
    for index, row in tqdm(df.iterrows()):

        mmr.append(metrics.mmr(row["correct_context"],row["retrieved_docs"]))
        precision.append(metrics.precison(row["correct_context"],row["retrieved_docs"]))
        recall.append(metrics.recall(row["correct_context"], row["retrieved_docs"]))
    f1 = metrics.F1(precision,recall)

    df["mmr"] = mmr
    df["precision"] = precision
    df["recall"] = recall
    df["f1"] = f1
    print(df.head())
    #df.to_csv(file, index=False)

def helper(val):
    return val.replace('""','"')

def helper(val):
    return val.replace('“','"')


if __name__ == "__main__":



    #file= "dbpedia_entity/semantic_queries.csv"
    #run_test(file)
    file = "runs/semantic_queries_2025-06-05-15-21.csv"
    #run_score(file)
    df = pd.read_csv(file)
    print(df[["mmr","precision","recall","f1"]].describe())