import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from Backend.evaluation import metrics

load_dotenv()

def score(file, type):
    if type == "function":
        df = pd.read_csv(file,converters={'retrieved_docs': pd.eval})
    elif type =="step" or type =="query" :
        df = pd.read_csv(file, converters={'retrieved_docs': pd.eval, 'products': pd.eval, "correct_context": pd.eval})
    df = df.dropna()

    exact_match = []

    for index, row in tqdm(df.iterrows()):
        if type =="function":
            exact_match.append(metrics.exact_match(row["response"], [row["function"]]))
        elif type =="step":
            exact_match.append(metrics.exact_match(row["response"], [row["product"]]))
        elif type =="query":
            exact_match.append(metrics.exact_match(row["response"], row["products"]))

    df["exact_match"] = exact_match
    df.to_csv(file, index=False)


if __name__ == "__main__":
    #createGroundTruth(file)
    file = "runs/function_function_reorder_2025-07-27-12-06.csv"
    #file = "bird_minidev_questions_functions_simple_eval_function_2025-06-17-17-36.csv"
    #score_function_no_reorder(file)
    scoreFunction(file)
    #score_step(file)
    #score(file)
    df = pd.read_csv(file)
    print(df[["exact_match"]].describe())

    #file = "runs/query_multilevel_reorder_05_2025-07-22-10-09.csv"
    #score(file)
    #df = pd.read_csv(file)
    #print(df[["exact_match"]].describe())
    #print(df[["simple_match"]].describe())
    #print(df[[ "ragasFaithfullnes","ragasResponseGroundedness",
    #          "ragasSemanticSimilarity"]].describe())