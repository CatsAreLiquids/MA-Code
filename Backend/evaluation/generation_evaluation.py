import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from Backend.evaluation import metrics


load_dotenv()

def score(file, type):
    if type == "function":
        df = pd.read_csv(file,converters={'retrieved_docs': pd.eval,"transformations":pd.eval})
    elif type =="step" or type =="query" :
        df = pd.read_csv(file, converters={'retrieved_docs': pd.eval, 'products': pd.eval, "correct_context": pd.eval})
    multiple_results = False

    exact_match = []
    for index, row in tqdm(df.iterrows()):
        if type =="function":
            if multiple_results:
                tmp_score = 0
                transformations = row["transformations"]
                for transformation in transformations:
                    tmp_score += metrics.exact_match_multiple(row["response"],transformation)

                exact_match.append(tmp_score/len(transformations))
            else:
                exact_match.append(metrics.exact_match(row["response"], [row["function"]]))

        elif type =="step":
            exact_match.append(metrics.exact_match(row["response"], [row["product"]]))
        elif type =="query":
            exact_match.append(metrics.exact_match(row["response"], row["products"]))

    df["exact_match"] = exact_match
    df.to_csv(file, index=False)


if __name__ == "__main__":
    file = "tmp/query_hybrid_reorder_05_2025-07-20-20-06.csv"
    score(file, "function")
