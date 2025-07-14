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
import sql_results

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
        if (ref == tmp).all(1).any():
            tp += 1
        else:
            fp += 1

    for i in range(ref.shape[0]):
        tmp = np.asarray(ref.iloc[i])
        if (test == tmp).all(1).any():
            pass
        else:
            fn += 1

    return tp / (tp + fp), tp / (tp + fn)


def eval_columns(ref, test):
    ref = ref.to_list()
    test = test.to_list()


def test_plan(file):
    df = pd.read_csv(file)
    df.dropna(how= "any", inplace=True)
    precision = []
    recall = []
    time_ = []
    p = {"plans":[{"function":"http://127.0.0.1:5200/retrieve","filter_dict":{"product":"http://127.0.0.1:5000/products/card_games/cards"}},{"function":"http://127.0.0.1:5200/filter","filter_dict":{"conditions":{"cardKingdomFoilId":"not empty","cardKingdomId":"not empty"}}}]}
    t = sql_results.res_dict["340"]
    pre, re = eval_rows(t, p)
    """
    for index, row in df.iterrows():
        start = time.time()
        p = execute.execute_new(ast.literal_eval(row["plan"]))
        end = time.time()
        t = sql_results.res_dict[str(row["question_id"])]
        pre, re = eval_rows(t, p)
        #except:
        #    print("meh")
        #    pre= 0
        #    re = 0
        precision.append(pre)
        recall.append(re)
        time_.append(end-start)
        print(end-start)
    """
    df["precision"] = precision
    df["recall"] = recall

    df.to_csv(file, index=False)


def test_plan_generation(file):
    df = pd.read_csv(file)
    df = df.dropna()
    df["plan"] = df["plan"].apply(lambda x: str(x).replace("\"\"", "\""))

    agent = init_agent()

    res = {"response": [], "agent_error": [], "ast_error": [], "executer_error": [], "planRecall": [], "jaccard": [],
           "planPrecision": [], "schemaAdherence": [], "time": []}

    for index, row in tqdm(df.iterrows()):

        try:
            start = time.time()
            response = agent.invoke({'input': row["query"]})['output']

            end = time.time()
            res["time"].append(end - start)
            res["response"].append(response)
            res["agent_error"].append(0)
        except:
            res["response"].append("")
            res["agent_error"].append(1)
            res["ast_error"].append(np.NaN)
            res["executer_error"].append(np.NaN)
            res["planRecall"].append(np.NaN)
            res["jaccard"].append(np.NaN)
            res["planPrecision"].append(np.NaN)
            res["schemaAdherence"].append(np.NaN)
            res["time"].append(np.NaN)
            continue

        try:
            response = ast.literal_eval(response)
            res["ast_error"].append(0)
            res["planRecall"].append(metrics.planRecall(response, row["plan"]))
            res["jaccard"].append(metrics.jaccard(response, row["plan"]))
            res["planPrecision"].append(metrics.planPrecision(response, row["plan"]))
            res["schemaAdherence"].append(metrics.schemaAdherence(response))
        except:
            res["executer_error"].append(np.NaN)
            res["ast_error"].append(1)
            res["planRecall"].append(np.NaN)
            res["jaccard"].append(np.NaN)
            res["planPrecision"].append(np.NaN)
            res["schemaAdherence"].append(np.NaN)
            continue

        res["executer_error"].append(np.NaN)

    print(res)
    # TOdo execute and compare results

    res_df = pd.DataFrame(res)

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    res_df.to_csv(f"prototype_eval_{timestamp}.csv", index=False)


if __name__ == "__main__":
    file = "bird_mini_dev/prototype_eval.csv"
    # runTest(file)
    test_plan(file)
    #file = "prototype_eval_2025-06-27-11-04.csv"
    #df = pd.read_csv(file)
    # print(df.dtypes)
    # print(df[["agent_error", "ast_error", "executer_error", "time"]].mean())
    # print(df[["planRecall", "jaccard", "planPrecision", "schemaAdherence"]].mean())
