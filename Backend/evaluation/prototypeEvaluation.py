import pandas as pd
from Backend.Agent.Agent import init_agent
from Backend.Agent import execute
from dotenv import load_dotenv
from Backend.evaluation import metrics
import ast
from tqdm import tqdm
from datetime import datetime
import time
import numpy as np
import duckdb
import requests
import json
import io

load_dotenv()
pd.set_option('display.max_columns', None)

def eval_rows(ref, test):
    tp = 0
    fp = 0
    fn = 0

    for i in range(test.shape[0]):
        test_tmp = np.asarray(test.iloc[i])
        for i in range(ref.shape[0]):
            ref_tmp = np.asarray(test.iloc[i])
            if np.array_equal(test_tmp, ref_tmp):
                tp += 1
            else:
                fp += 1

    for i in range(ref.shape[0]):
        ref_tmp = np.asarray(ref.iloc[i])
        for i in range(test.shape[0]):
            test_tmp = np.asarray(test.iloc[i])
            if not np.array_equal(test_tmp, ref_tmp):
                fn += 1

    try :
        p = tp / (tp + fp)
    except ZeroDivisionError:
        p = 0

    try :
        r = tp / (tp + fn)
    except ZeroDivisionError:
        r = 0

    return p, r


def eval_by_index(ref, test,columns):
    #print(ref, test,columns)
    ref = set(ref[columns].tolist())
    test = set(test[columns].tolist())


    tp = len(ref & test)
    fn = len(ref - test)
    fp = len(test - ref)


    return tp / (tp + fp), tp / (tp + fn)

def test_plan(file):
    df = pd.read_csv(file)
    #df= df[df["question_id"]==671]
    precision = []
    recall = []
    execution_error = []


    for index, row in tqdm(df.iterrows()):

        try:
            p = execute.execute_new(ast.literal_eval(row["response"]))
            p = p.replace({np.nan: "None"})

            t = execute.execute_new(ast.literal_eval(row["plan"]))
            t = t.replace({np.nan: "None"})

            if isinstance(p, pd.DataFrame):
                p = p.reindex(sorted(p.columns), axis=1)

            if isinstance(t, pd.DataFrame):
                t = t.reindex(sorted(t.columns), axis=1)


            if row['type'] == "index":
                pre, re = eval_by_index(t, p,row['columns'])
            else:
                pre, re = eval_rows(t, p)
            ex_error = 0
        except:
            print("???")
            ex_error = 1
            pre = 0
            re = 0

        precision.append(pre)
        recall.append(re)
        execution_error.append(ex_error)
        #print(end-start)

    print(precision,recall)
    df["precision"] = precision
    df["recall"] = recall
    df["execution_error"] = execution_error
    df.to_csv(file, index=False)


def generate_plan():

    df = pd.read_csv("bird_mini_dev/prototype_eval.csv")
    df = df.dropna()

    res = {"response": [], "agent_error": [], "time": []}
    agent = init_agent(dual_prompt=True)

    for index, row in tqdm(df.iterrows()):
        try:
            start = time.time()
            response = agent.invoke({'query': row["query"],"evidence":row["evidence"]})['output']

            end = time.time()
            res["time"].append(end - start)
            res["response"].append(response)
            res["agent_error"].append(0)
        except:
            end = time.time()
            print(row["question_id"])
            res["response"].append("")
            res["agent_error"].append(1)
            res["time"].append(end - start)


    df["response"] = res["response"]
    df["agent_error"] = res["agent_error"]
    df["agent_time"] = res["time"]
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    df.to_csv(f"prototype_eval_column_info_{timestamp}.csv", index=False)

def eval_plan(file):
    df = pd.read_csv(file)

    res = {"planRecall": [], "jaccard": [],"planPrecision": []}

    for index, row in tqdm(df.iterrows()):
        try:
            res["planRecall"].append(metrics.planRecall(row["response"], row["plan"]))
            res["jaccard"].append(metrics.jaccard(row["response"], row["plan"]))
            res["planPrecision"].append(metrics.planPrecision(row["response"], row["plan"]))

        except:
            res["planRecall"].append(np.NaN)
            res["jaccard"].append(np.NaN)
            res["planPrecision"].append(np.NaN)

    df["planRecall"] = res["planRecall"]
    df["jaccard"] = res["jaccard"]
    df["planPrecision"] = res["planPrecision"]

    df.to_csv(file, index=False)


if __name__ == "__main__":
    #generate_plan()

    file = "no_explanation_no_example.csv"
    #eval_plan(file)
    #test_plan(file)
    df = pd.read_csv(file)


    print(df[["agent_error", "agent_time"]].describe())
    print(df[["planRecall", "jaccard", "planPrecision"]].describe())
    print(df[["recall", "precision","execution_error"]].describe())
    #df = df.sort_values(by=["precision", "recall"], ascending=False)
    #print(df[df["execution_error"] != 1])

    #df = df.sort_values(by=["planPrecision","planRecall"], ascending=False)
    #print(df[df["execution_error"] == 1].head(n=10))

