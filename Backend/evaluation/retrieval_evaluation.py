import ast

from Backend.RAG import vector_db
from Backend import models
from Backend.evaluation import metrics
import pandas as pd
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
from numpy.random import default_rng
from Backend.RAG.eval_retriever import productRetriever_eval,functionRetriever_eval, multilevelRetriever, functionRetriever_hybrid,productRetriever_eval_both
import time


functions = {"product": productRetriever_eval, "function": functionRetriever_eval,"function_text":functionRetriever_eval, "multilevel":multilevelRetriever,"hybrid":functionRetriever_hybrid,
             "both":productRetriever_eval_both}

def run_test(config):
    df = pd.read_csv(config["file"],converters={'retrieved_docs': pd.eval,'products': pd.eval,"correct_context":pd.eval})

    answer, context = [],[]
    for index, row in tqdm(df.iterrows()):

        res = functions[config["retriever"]](row["query"])
        answer.append(res["response"])
        context.append(res["docs"])
        print(res["docs"])

    df["response"] = answer
    df["retrieved_docs"] = context

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    df.to_csv(f"runs/query_multilevel_no_reorder_{timestamp}.csv",index=False)

def run_test_step(config):
    df = pd.read_csv(config["file"],converters={'retrieved_docs': pd.eval,'products': pd.eval,"correct_context":pd.eval})

    answer, context = [],[]
    for index, row in tqdm(df.iterrows()):
        res = functions[config["retriever"]](row["step"],)
        answer.append(res["response"])
        context.append(res["docs"])

    df["response"] = answer
    df["retrieved_docs"] = context

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    df.to_csv(f"runs/step_multilevel_reorder_{timestamp}.csv",index=False)

def run_test_step_both(config):
    df = pd.read_csv(config["file"],
                     converters={'retrieved_docs': pd.eval, 'products': pd.eval, "correct_context": pd.eval})

    answer, context = [], []
    for index, row in tqdm(df.iterrows()):
        res = functions[config["retriever"]](row["question_id"],row["step"])
        answer.append(res["response"])
        context.append(res["docs"])

    df["response"] = answer
    df["retrieved_docs"] = context

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    df.to_csv(f"runs/both_multilevel_reorder_{timestamp}.csv", index=False)

def run_score_steps(file):
    #df = pd.read_csv(file)
    #print(df.head())
    df = pd.read_csv(file,converters={'retrieved_docs': pd.eval,'names': pd.eval})
    mmr = []
    precision = []
    recall = []


    for index, row in tqdm(df.iterrows()):
        mmr.append(metrics.mmr([row["product"]],row["retrieved_docs"]))
        precision.append(metrics.precison([row["product"]],row["retrieved_docs"]))
        recall.append(metrics.recall([row["product"]], row["retrieved_docs"]))

    f1 = metrics.F1(precision,recall)

    df["mmr"] = mmr
    df["precision"] = precision
    df["recall"] = recall
    df["f1"] = f1
    df.to_csv(file, index=False)

def run_score(file):
    #df = pd.read_csv(file)
    #print(df.head())
    df = pd.read_csv(file,converters={'retrieved_docs': pd.eval,'names': pd.eval,"products":pd.eval,})
    mmr = []
    precision = []
    recall = []


    for index, row in tqdm(df.iterrows()):
        mmr.append(metrics.mmr(row["products"],row["retrieved_docs"]))
        precision.append(metrics.precison(row["products"],row["retrieved_docs"]))
        recall.append(metrics.recall(row["products"], row["retrieved_docs"]))

    f1 = metrics.F1(precision,recall)

    df["mmr"] = mmr
    df["precision"] = precision
    df["recall"] = recall
    df["f1"] = f1
    df.to_csv(file, index=False)

def scoreFunction(file):
    df = pd.read_csv(file,converters={'retrieved_docs': pd.eval})
    mmr = []
    precision = []
    recall = []
    hit_rate = []

    for index, row in tqdm(df.iterrows()):
        hit_rate.append(metrics.hit_rate_func([row["function"]],row["retrieved_docs"]))
        mmr.append(metrics.mmr_func([row["function"]], row["retrieved_docs"]))
        precision.append(metrics.precison_func([row["function"]],row["retrieved_docs"]))
        recall.append(metrics.recall_func([row["function"]], row["retrieved_docs"]))

    f1 = metrics.F1(precision,recall)

    df["mmr"] = mmr
    df["hit_rate"] = hit_rate
    df["precision"] = precision
    df["recall"] = recall
    df["f1"] = f1


    df.to_csv(file, index=False)

def runFunction(config):
    df = pd.read_csv(config["file"])
    df = df.dropna()

    answer, context = [],[]
    for index, row in tqdm(df.iterrows()):
        if row["function"] != "":
            res = functions["function"](row["step"],"function")
            answer.append(res["response"])
            context.append(res["docs"])

    df["response"] = answer
    df["retrieved_docs"] = context

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    df.to_csv(f"runs/function_hybrid_hybrid_reorder_{timestamp}.csv",index=False)

def helper(org,functionName):

    if pd.notna(org):
        org = ast.literal_eval(org)
        text = f"function name:{functionName}\n"
        for k, v in org.items():
            text += f"{k}: {v}\n"

        return text

    else:
        return ""

if __name__ == "__main__":
    #file = "bird_mini_dev/bird_minidev_questions_eval_multilevel_2025-06-20-10-09_07.csv"
    #df = pd.read_csv(file)
    #tmp =[]
    #for index, row in tqdm(df.iterrows()):
    #    tmp.append(helper(row["correct_context"],row["function"]))
    #df["correct_context"] = tmp
    #df.to_csv(file,index=False)
    file = "runs/function_hybrid_hybrid_reorder_03_2025-07-27-14-33.csv"
    #run_score_steps(file)
    #run_score(file)
    scoreFunction(file)
    df = pd.read_csv(file)
    print(df[["mmr", "precision", "recall", "f1"]].describe())

    config = {"file":"bird_mini_dev/functions_simplel.csv","num_of_querys":111,"retriever":"function"}

    #file= "dbpedia_entity/semantic_queries.csv"
    file = "bird_mini_dev/bird_minidev_questions_eval.csv"
    start = time.time()
    #run_test(config)
    #run_test_step_both(config=config)
    #run_test_step(config=config)
    #runFunction(config)
    end = time.time()
    print(end - start)
