import json
import re
import sqlite3
import pandas as pd

def extract():
    db_file="MINIDEV/dev_databases/formula_1/formula_1.sqlite"
    conn = sqlite3.connect(db_file, isolation_level=None,
                       detect_types=sqlite3.PARSE_COLNAMES)
    db_df = pd.read_sql_query("SELECT * FROM molecule", conn)
    db_df.to_csv('molecule.csv', index=False)

def remove(prods):
    while ("time_in_seconds" in prods) or ("last_driver_incremental" in prods) or ("champion_time" in prods) or ("lap_times_in_seconds" in prods) or ("fastest_lap_times" in prods):
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

    res_dict= {"question_id":[],"query":[],"difficulty":[],"products":[],"collection":[]}

    for d in data:
        sql = d["SQL"]
        prods = re.findall(r"(?i)(?:from|join)\s*`(.*?)`",sql)
        res_dict["question_id"].append(d["question_id"])
        res_dict["query"].append(d["question"])
        res_dict["difficulty"].append(d["difficulty"])
        res_dict["collection"].append(d["db_id"])

        prods = remove(prods)

        res_dict["products"].append(list(set(prods)))




    df = pd.DataFrame(res_dict)
    df["type"] ="product"
    df.to_csv("bird_minidev_questions.csv",index=False)

def createEval():
    df = pd.read_csv("bird_minidev_questions.csv", converters={'products': pd.eval})

    correc_context = []
    for index, row in df.iterrows():
        tmp =[]
        for i in row["products"]:
            data = json.load(open(f"../../data/{row['collection']}/metadata_automatic.json"))
            tmp.append(data[i]["description"])
        correc_context.append(tmp)

    df["correct_context"] = correc_context
    df.to_csv("bird_minidev_questions.csv", index=False)

if __name__ == "__main__":
    #createTestFile()
    createEval()
    pass