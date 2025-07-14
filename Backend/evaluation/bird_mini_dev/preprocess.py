import json
import re
import sqlite3
import pandas as pd
from numpy.random import default_rng
from Backend.Agent import Agent
from dotenv import load_dotenv
import yaml

load_dotenv()

def extract():
    db_file = "MINIDEV/dev_databases/formula_1/formula_1.sqlite"
    conn = sqlite3.connect(db_file, isolation_level=None,
                           detect_types=sqlite3.PARSE_COLNAMES)
    db_df = pd.read_sql_query("SELECT * FROM molecule", conn)
    db_df.to_csv('molecule.csv', index=False)


def remove(prods):
    while ("time_in_seconds" in prods) or ("last_driver_incremental" in prods) or ("champion_time" in prods) or (
            "lap_times_in_seconds" in prods) or ("fastest_lap_times" in prods):
        if "time_in_seconds" in prods:
            print(prods)
            prods.remove("time_in_seconds")
            print(prods)
        if "last_driver_incremental" in prods:
            prods.remove("last_driver_incremental")
        if "champion_time" in prods:
            prods.remove("champion_time")
        if "lap_times_in_seconds" in prods:
            prods.remove("lap_times_in_seconds")
        if "fastest_lap_times" in prods:
            prods.remove("fastest_lap_times")

    return prods


def createTestFile():
    with open('mini_dev_mysql.json') as f:
        data = json.load(f)

    res_dict = {"question_id": [], "query": [], "difficulty": [], "products": [], "collection": []}

    for d in data:
        sql = d["SQL"]
        prods = re.findall(r"(?i)(?:from|join)\s*`(.*?)`", sql)
        res_dict["question_id"].append(d["question_id"])
        res_dict["query"].append(d["question"])
        res_dict["difficulty"].append(d["difficulty"])
        res_dict["collection"].append(d["db_id"])

        prods = remove(prods)

        res_dict["products"].append(list(set(prods)))

    df = pd.DataFrame(res_dict)
    df["type"] = "product"
    df.to_csv("bird_minidev_questions.csv", index=False)


def createEval():
    df = pd.read_csv("bird_minidev_questions.csv", converters={'products': pd.eval})

    correc_context = []
    for index, row in df.iterrows():
        tmp = []
        for i in row["products"]:
            data = json.load(open(f"../../data/{row['collection']}/metadata_automatic.json"))
            tmp.append(data[i]["description"])
        correc_context.append(tmp)

    df["correct_context"] = correc_context
    df.to_csv("bird_minidev_questions.csv", index=False)

def createAnswers(query):
    agent= newnew_restart.init_agent()
    try:
        agent_result = agent.invoke({'input': query})['output']
        return agent_result
    except:
        return None

def createFunc_Gen_Eval():
    data = json.load(open("mini_dev_mysql.json"))
    df = pd.DataFrame(data)
    vals = ["moderate", "simple", "challenging"]

    idxs = []
    for val in vals:
        df_tmp = df[df["difficulty"] == val]
        rng = default_rng()
        numbers = rng.choice(len(df_tmp), size=20, replace=False)
        idx_tmp = df_tmp.index[numbers]
        idxs+= idx_tmp.to_list()

    df = df.loc[idxs]
    df = df.drop(columns=["evidence","SQL"])
    df = df.rename(columns={"question": "query", "db_id": "collection"})
    df["generated_answer"] = df["query"].apply(lambda x: createAnswers(x))
    df["corrected_answer"] = df["generated_answer"]
    df.to_csv("bird_minidev_questions_subset.csv",index =False)

def createTestset():
    df = pd.read_csv("bird_minidev_questions.csv")
    df = df.drop(columns=["correc_context"])
    counts = [3,5,5,2,5,3,1,6,3,2,5,3,3,5,2,1,5,4,1,5,4,3,4,1,1,5,1,1,2,3,1,3,2]
    keys = [("formula_1","challenging"),("formula_1","moderate"),("formula_1","simple"),("superhero","challenging"),("superhero","moderate"),("superhero","simple"),
            ("card_games","challenging"),("card_games","moderate"),("card_games","simple"),("european_football_2","challenging"),("european_football_2","moderate"),("european_football_2","simple"),
            ("thrombosis_prediction","challenging"),("thrombosis_prediction","moderate"),("thrombosis_prediction","simple"),("codebase_community","challenging"),("codebase_community","moderate"),("codebase_community","simple"),
            ("student_club","challenging"),("student_club","moderate"),("student_club","simple"),("toxicology","challenging"),("toxicology","moderate"),("toxicology","simple"),
            ("financial","challenging"),("financial","moderate"),("financial","simple"),("debit_card_specializing","challenging"),("debit_card_specializing","moderate"),("debit_card_specializing","simple"),
            ("california_schools","challenging"),("california_schools","moderate"),("california_schools","simple")]

    idxs_eval = []
    idxs_train = []
    for i in range(len(counts)):
        mask = ((df["difficulty"] == keys[i][1])  & (df["collection"] == keys[i][0]))
        df_tmp = df[mask]
        rng = default_rng()
        numbers = rng.choice(len(df_tmp), size=counts[i], replace=False)
        idx = df_tmp.index.to_list()
        idx_tmp = df_tmp.index[numbers].to_list()
        for i in idx_tmp:
            idx.remove(i)
        idxs_eval += idx_tmp
        idxs_train += idx

    df_eval = df.loc[idxs_eval]
    df_train = df.loc[idxs_train]

    df_eval.to_csv("bird_minidev_questions_eval.csv",index =False)
    df_train.to_csv("bird_minidev_questions_train.csv", index=False)

def createBreakdown(query):
    res = Agent.breakDownQuery.invoke(query)
    print(res)
    return

def helper(plan):
    print(plan)

    prods = re.findall(r"(?:\d{1, 2}\.(.* ?)\d{1, 2}\.)", plan)
    print(prods)
    return prods


def create_function_retrieval(file):
    df = pd.read_csv(file)
    df = df[df["difficulty"]=="moderate"]

    res_dict = {"question_id": [], "query": [], "difficulty": [], "function": [], "collection": [], "step":[]}

    for index, row in df.iterrows():
        plan = Agent.breakDownQuery.invoke(row["query"])
        steps = re.findall(r"\*\*(.* ?)\*\*", plan.content)
        for step in steps:
            res_dict["question_id"].append(row["question_id"])
            res_dict["query"].append(row["query"])
            res_dict["difficulty"].append(row["difficulty"])
            res_dict["collection"].append(row["collection"])
            res_dict["function"].append("")
            res_dict["step"].append(step)

    res_df = pd.DataFrame(res_dict)
    res_df.to_csv("bird_minidev_questions_functions_moderate_eval.csv")

def collectContext():
    df = pd.read_csv("bird_minidev_questions_functions_challenging_eval.csv")

    with open("../../data/Catalogs/function_catalog.yml") as stream:
        catalog = yaml.safe_load(stream)

    correc_context = []
    for index, row in df.iterrows():
        correc_context.append(catalog[row["function"]])



    df["correct_context"] = correc_context
    #df.to_csv("bird_minidev_questions_functions_simple_eval.csv", index=False)

def ground_truth_sql():
    df1 = pd.read_csv("bird_minidev_questions.csv")
    df2 = pd.read_csv("prototype_eval.csv")

    res_df = df1.join(df2, rsuffix="_r",how="inner")
    res_df = res_df.drop(columns=['plan','correc_context', 'correct_context', 'question_id_r', 'query_r',
                                  'difficulty_r','description ','type','collection','difficulty'])

    data = json.load(open("mini_dev_mysql.json"))
    df = pd.DataFrame(data)

    res_df = res_df.join(df, rsuffix="_r", how="inner")
    print(res_df.columns)
    res_df = res_df.drop(columns=['dataset', 'question', 'evidence', 'question_id_r','difficulty' ])
    print(res_df.columns)
    res_df.to_csv("product_sql.csv",index=False)




if __name__ == "__main__":
    # createTestFile()
    # createEval()
    #createFunc_Gen_Eval()
    df = pd.read_csv("bird_minidev_questions.csv")
    #ground_truth_sql()
    #createTestset()
    #create_function_retrieval("bird_minidev_questions_eval.csv")
    print(createBreakdown("Rank schools by their average score in Writing where the score is greater than 499, showing their charter numbers."))
    #collectContext()
