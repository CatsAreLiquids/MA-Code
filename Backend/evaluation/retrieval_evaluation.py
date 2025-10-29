import ast
from datetime import datetime

import pandas as pd
from tqdm import tqdm

from Backend.RAG.evaluation_retriever import multilevelRetriever, functionRetriever_hybrid, functionRetriever_eval, \
    productRetriever_eval_both, productRetriever_eval
from Backend.evaluation import metrics

functions = {"product": productRetriever_eval, "function": functionRetriever_eval, "multilevel":multilevelRetriever,"hybrid":functionRetriever_hybrid,
             "both":productRetriever_eval_both}

def run_product_query(config):
    df = pd.read_csv(config["file"],converters={'retrieved_docs': pd.eval,'products': pd.eval,"correct_context":pd.eval})

    answer, context = [],[]
    for index, row in tqdm(df.iterrows()):

        res = functions[config["retriever"]](row["query"])
        answer.append(res["response"])
        context.append(res["docs"])

    df["response"] = answer
    df["retrieved_docs"] = context

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    df.to_csv(f"runs/query_multilevel"
              f"_reorder_{timestamp}.csv",index=False)

def run_product_step(config):
    df = pd.read_csv(config["file"],converters={'retrieved_docs': pd.eval,'products': pd.eval,"correct_context":pd.eval})

    answer, context = [],[]
    for index, row in tqdm(df.iterrows()):
        res = functions[config["retriever"]](row["step"],)
        answer.append(res["response"])
        context.append(res["docs"])

    df["response"] = answer
    df["retrieved_docs"] = context

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    df.to_csv(f"runs/step_multilevel_no_reorder_{timestamp}.csv",index=False)

def run_product_both(config):
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

def run_transformations(config):
    df = pd.read_csv(config["file"])

    answer, context = [],[]
    for index, row in tqdm(df.iterrows()):
        res = functions["function"](row["query"],"function")
        answer.append(res["response"])
        context.append(res["docs"])

    df["response"] = answer
    df["retrieved_docs"] = context

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    df.to_csv(f"runs/query_hybrid_hybrid_no_reorder_{timestamp}.csv",index=False)

def score_product_steps(file):
    df = pd.read_csv(file,converters={'retrieved_docs': pd.eval,'names': pd.eval})
    mmr = []
    hit_rate = []


    for index, row in tqdm(df.iterrows()):
        mmr.append(metrics.mmr([row["product"]],row["retrieved_docs"]))
        hit_rate.append(metrics.hit_rate([row["product"]], row["retrieved_docs"]))

    df["mmr"] = mmr
    df["hit_rate"] = hit_rate
    df.to_csv(file, index=False)

def score_product_query(file):
    df = pd.read_csv(file,converters={'retrieved_docs': pd.eval,'names': pd.eval,"products":pd.eval,})
    mmr = []
    hit_rate = []


    for index, row in tqdm(df.iterrows()):
        mmr.append(metrics.mmr(row["products"],row["retrieved_docs"]))
        hit_rate.append(metrics.hit_rate(row["products"], row["retrieved_docs"]))


    df["mmr"] = mmr
    df["hit_rate"] = hit_rate
    df.to_csv(file, index=False)

def score_transformations(file,style):
    df = pd.read_csv(file,converters={'retrieved_docs': pd.eval,'transformations': pd.eval})
    mmr = []
    hit_rate = []
    if style == "query":
        for index, row in tqdm(df.iterrows()):
            tmp_hit_rate = 0
            tmp_mmr = 0

            transformations = row["transformations"]
            for transformation in transformations:

                tmp_hit_rate += metrics.hit_rate_func(transformation,row["retrieved_docs"])
                tmp_mmr += metrics.mmr_func(transformation, row["retrieved_docs"])

            hit_rate.append(tmp_hit_rate / len(row["transformations"]))
            mmr.append(tmp_mmr/len(row["transformations"]))

    else:
        for index, row in tqdm(df.iterrows()):
            hit_rate.append(metrics.hit_rate_func(row["transformations"],row["retrieved_docs"]))
            mmr.append(metrics.mmr_func(row["transformations"], row["retrieved_docs"]))


    df["mmr"] = mmr
    df["hit_rate"] = hit_rate

    df.to_csv(file, index=False)


if __name__ == "__main__":

    config = {"file":"test_files/products_retrieval_steps.csv","retriever":"product"}
    run_test_step(config)
