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
load_dotenv()


def runTest(file):
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
            res["time"].append(end-start)
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
    #TOdo execute and compare results


    res_df = pd.DataFrame(res)

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    res_df.to_csv(f"prototype_eval_{timestamp}.csv", index=False)

if __name__ == "__main__":
    file = "bird_mini_dev/prototype_eval.csv"
    #runTest(file)
    file = "prototype_eval_2025-06-27-11-04.csv"
    df = pd.read_csv(file)
    print(df.dtypes)
    print(df[["agent_error","ast_error", "executer_error","time"]].mean())
    print(df[["planRecall", "jaccard", "planPrecision", "schemaAdherence"]].mean())
