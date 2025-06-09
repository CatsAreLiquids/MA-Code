from Backend.RAG import vector_db
from Backend import models
from Backend.evaluation import metrics
import pandas as pd
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
from numpy.random import default_rng


functions = {"product": productRetriever, "function": functionRetriever,"function_text":functionRetriever, "db_pedia": db_pediaRetriever,}

def run_test(config:Dict, file,num_of_querys=None):
    df = pd.read_csv(file,converters={'retrieved_docs': pd.eval,'products': pd.eval,"correct_context":pd.eval})


    if num_of_querys is not None:
        if num_of_querys > len(df):
            print(f"Less querys than askewd for, file has max {len(df)} queries. Wont proceed")
            return
        else:
            rng = default_rng()
            numbers = rng.choice(len(df), size=num_of_querys, replace=False)

    df_tmp = df.loc[numbers]
    answer, context = [],[]
    for index, row in tqdm(df_tmp.iterrows()):
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
        else:
            answer.append([""])
            context.append([""])

    df_tmp["response"] = answer
    df_tmp["retrieved_docs"] = context

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
    df_tmp.to_csv(f"runs/{Path(Path(file).name).stem}_{timestamp}.csv",index=False,encoding='utf8')

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
    df.to_csv(file, index=False)



if __name__ == "__main__":
    config = {"file":"bird_mini_dev/bird_minidev_questions.csv","num_of_querys":None,"retriever":"multilevel"}

    #file= "dbpedia_entity/semantic_queries.csv"
    file = "bird_mini_dev/bird_minidev_questions.csv"
    #run_test(file,10)
    file = "runs/bird_minidev_questions_2025-06-09-11-08.csv"
    run_score(file)
    df = pd.read_csv(file)
    print(df[["mmr","precision","recall","f1"]].describe())