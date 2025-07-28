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


def _get_df(product, collection):
    response = requests.get(f"http://127.0.0.1:5000/products/{collection}/{product}")
    content = json.loads(response.text)
    df = pd.read_json(io.StringIO(content['data']))
    return df


def eval_rows(ref, test):
    tp = 0
    fp = 0
    fn = 0


    for i in range(test.shape[0]):
        tmp = np.asarray(test.iloc[i])
        try:
            if (ref == tmp).all(1).any():
                tp += 1
            else:
                fp += 1
        except ValueError:
            if (ref == tmp).all().any():
                tp += 1
            else:
                fp += 1

    for i in range(ref.shape[0]):
        tmp = np.asarray(ref.iloc[i])
        try:
            if (test == tmp).all(1).any():
                pass
            else:
                fn += 1
        except ValueError:
            print(test.shape,tmp.shape)
            if (test == tmp).all().any():
                pass
            else:
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
    #df.dropna(how= "any", inplace=True)
    precision = []
    recall = []
    execution_error = []


    for index, row in df.iterrows():

        try:
            start = time.time()
            p = execute.execute_new(ast.literal_eval(row["response"]))
            p = p.replace({np.nan: "None"})

            end = time.time()
            t = execute.execute_new(ast.literal_eval(row["plan"]))
            t = t.replace({np.nan: "None"})
            if row['type'] == "index":
                pre, re = eval_by_index(t, p,row['columns'])
            else:
                pre, re = eval_rows(t, p)
            ex_error = 0
        except:
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
    agent = init_agent()

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
    df.to_csv(f"prototype_eval_{timestamp}.csv", index=False)

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
    file = "prototype_eval_2025-07-28-18-27.csv"
    eval_plan(file)
    test_plan(file)
    df = pd.read_csv(file)
    print(df[["agent_error", "agent_time"]].describe())
    print(df[["planRecall", "jaccard", "planPrecision"]].describe())
    print(df[["recall", "precision","execution_error"]].describe())
    print(df[df["execution_error"] != 1])