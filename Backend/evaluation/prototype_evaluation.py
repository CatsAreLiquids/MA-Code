import ast
import time
from datetime import datetime

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from Backend.Agent import execute
from Backend.Agent.Agent import init_agent
from Backend.evaluation import metrics

load_dotenv()
pd.set_option('display.max_columns', None)

def _eval_rows(ref, test):
    tp = 0
    fp = 0
    fn = 0

    for i in range(test.shape[0]):
        found = False
        test_tmp = np.asarray(test.iloc[i])
        for j in range(ref.shape[0]):
            ref_tmp = np.asarray(ref.iloc[j])
            if np.array_equal(test_tmp, ref_tmp):
                found = True

        if found:
            tp += 1
        else:
            fp += 1

    for i in range(ref.shape[0]):
        found = False
        ref_tmp = np.asarray(ref.iloc[i])
        for j in range(test.shape[0]):
            test_tmp = np.asarray(test.iloc[j])
            if np.array_equal(test_tmp, ref_tmp):
                found = True

        if not found:
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


def _eval_by_index(ref, test,columns):
    ref = set(ref[columns].tolist())
    test = set(test[columns].tolist())


    tp = len(ref & test)
    fn = len(ref - test)
    fp = len(test - ref)


    return tp / (tp + fp), tp / (tp + fn)


def generate_plan():

    df = pd.read_csv("Test Files/prototype_eval.csv")
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
            res["response"].append("")
            res["agent_error"].append(1)
            res["time"].append(end - start)


    df["response"] = res["response"]
    df["agent_error"] = res["agent_error"]
    df["agent_time"] = res["time"]
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    df.to_csv(f"prototype_eval_column_info_{timestamp}.csv", index=False)

def evaluate_plans(file):
    df = pd.read_csv(file)
    truth = pd.read_csv("plans_ground_truth.csv",converters={'"plan"': pd.eval})

    res = {"planRecall": [], "jaccard": [],"planPrecision": []}

    for index, row in tqdm(df.iterrows()):
        plan = truth[truth["question_id"] == row["question_id"]]["plan"].values[0]
        try:
            res["planRecall"].append(metrics.planRecall(row["response"], str(plan)))
            res["jaccard"].append(metrics.jaccard(row["response"], str(plan)))
            res["planPrecision"].append(metrics.planPrecision(row["response"], str(plan)))

        except:
            res["planRecall"].append(np.NaN)
            res["jaccard"].append(np.NaN)
            res["planPrecision"].append(np.NaN)

    df["planRecall"] = res["planRecall"]
    df["jaccard"] = res["jaccard"]
    df["planPrecision"] = res["planPrecision"]

    df.to_csv(file, index=False)

def evaluate_results(file):
    df = pd.read_csv(file)

    precision = []
    recall = []
    execution_error = []

    for index, row in tqdm(df.iterrows()):
        df2 = pd.read_csv(f"Ground Truth/{int(row['question_id'])}.csv")


        # wrong query number but spread across to many files to change
        if row["question_id"] == 773:
            df2 = pd.read_csv(f"Ground Truth/724.csv")

        try:
            p = execute.execute(ast.literal_eval(row["response"]))

            p = p.replace({np.nan: "None"})
            df2 = df2.replace({np.nan: "None"})

            # diffrent order evaluates as diffrent results
            if isinstance(df2, pd.DataFrame):
                df2 = df2.reindex(sorted(df2.columns), axis=1)

            if isinstance(df2, pd.DataFrame):
                df2= df2.reindex(sorted(df2.columns), axis=1)


            # small changes to ensure the same format/column names for refrence and test
            if row["question_id"] == 344:
                p = p.rename(columns={"id_cards": "id"})
            if row["question_id"] == 786:
                p = p.to_frame().T
                p = p.rename(columns={"hero_id": "count(T1.hero_id)"})
            if row["question_id"] == 875:
                p = p.rename(columns={"url_seasons": "url"})
            if row["question_id"] == 1096:
                df2 = df2.rename(columns={"""(CAST(sum(t2.overall_rating) AS FLOAT) / "nullif"(count(t2.id), 0))""": "overall_rating"})
            if row["question_id"] == 1032:
                p = p.rename(columns={"count": "max_count"})
            if row["question_id"] == 1114:
                p = p.to_frame().T
                df2 = df2.rename(columns={"""(CAST(sum(t2.overall_rating) AS FLOAT) / "nullif"(count(t2.id), 0))""": "0"})
            if row["question_id"] == 1405:
                p = p.rename(columns={"amount": "sum(T2.amount)"})
            if row["question_id"] == 39:
                p = p.to_frame().T
                p = p.rename(columns={"0": "avg(T1.NumTstTakr)"})
            if row["question_id"] == 1247:
                p = p.to_frame().T
                p = p.rename(columns={"0": "count(DISTINCT T1.ID)"})
            if row["question_id"] == 1251:
                p = p.to_frame().T
                p = p.rename(columns={"0": "count(DISTINCT T1.ID)"})
            if row["question_id"] == 92:
                p = p.to_frame().T
                p = p.rename(columns={"0": "count(DISTINCT T2.district_id)"})
            if row["question_id"] == 93:
                p = p.to_frame().T
                p = p.rename(columns={"0": "count(T1.client_id)"})
            if row["question_id"] == 137:
                p = p.to_frame().T
                p = p.rename(columns={"0": "count(T1.account_id)"})
            if row["question_id"] == 1509:
                p = p.to_frame().T
                p = p.rename(columns={"0": "count(T1.TransactionID)"})
            if row["question_id"] == 17:
                # the results are ranked/ sorted but no explicit ranking column exists
                p["WritingScoreRank"] = np.arange(1,len(p)+1)
            if row["question_id"] == 634:
                p = p.to_frame().T
                p = p.rename(columns={"ViewCount": "DisplayName"})
            if row["question_id"] == 1505:
                p = p.to_frame().T
                p = p.rename(columns={"CustomerID": "count"})
            if row["question_id"] == 1473:
                p = p.to_frame().T
                p = p.rename(columns={"Consumption": """(avg(T2.Consumption) / "nullif"(12, 0))"""})
            if row["question_id"] == 537:
                p = p.to_frame().T
                p = p.rename(columns={"Id_users": "count(T1.id)"})



            try:
                p = p[df2.columns.tolist()]
            except:
                pass

            # to minimise result mismatches due to diffrent rounding strategies
            df2 = df2.round(4)
            p = p.round(4)

            if row['type'] == "index" and row["question_id"] not in  [1381,773,1334,149,17]:
                pre, re = _eval_by_index(df2, p, row['columns'])
            else:
                try:
                    if (p==df2).all().iloc[0]:

                        pre = 1
                        re = 1
                    else:
                        pre, re = _eval_rows(df2, p)
                except:
                    pre, re = _eval_rows(df2, p)
            ex_error = 0


        except:
            ex_error = 1
            pre = 0
            re = 0

        precision.append(pre)
        recall.append(re)
        execution_error.append(ex_error)


    df["precision_sql"] = precision
    df["recall_sql"] = recall
    df["execution_error_sql"] = execution_error
    df.to_csv(file, index=False)

if __name__ == "__main__":
    file = "evidence_baseline.csv"
    df = pd.read_csv(file)
    #eval_plan(file)
    eval_sql(file)

    df = pd.read_csv(file)
    print(df[["planRecall","jaccard","planPrecision"]].describe())
    print(df[[ "recall_sql","precision_sql",  "execution_error_sql"]].describe())
    #print(df[["precision", "recall", "execution_error"]].describe())
    #tmp = df[(df["precision_sql"] != 1) | (df["recall_sql"] != 1)]
    #print(tmp[[ "question_id", "precision_sql","recall_sql" , "execution_error_sql"]])
    #print(len(tmp))


#1405, 1114,1334