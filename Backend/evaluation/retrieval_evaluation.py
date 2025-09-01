import ast
import time
from datetime import datetime

import pandas as pd
from tqdm import tqdm

from Backend.RAG.evaluation_retriever import multilevelRetriever, functionRetriever_hybrid, functionRetriever_eval, \
    productRetriever_eval_both, productRetriever_eval
from Backend.evaluation import metrics

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
    pass
