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
    pass