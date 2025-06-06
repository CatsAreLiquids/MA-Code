import json
import re
import sqlite3
import pandas as pd

def extract():
    db_file="MINIDEV/dev_databases/toxicology/toxicology.sqlite"
    conn = sqlite3.connect(db_file, isolation_level=None,
                       detect_types=sqlite3.PARSE_COLNAMES)
    db_df = pd.read_sql_query("SELECT * FROM molecule", conn)
    db_df.to_csv('molecule.csv', index=False)

def createTestFile():
    with open('mini_dev_mysql.json') as f:
        data = json.load(f)

    res_dict= {"question_id":[],"question":[],"difficulty":[],"products":[]}

    for d in data:
        sql = d["SQL"]
        prods = re.findall(r"(?i)(?:from|join)\s*`(.*?)`",sql)
        res_dict["question_id"].append(d["question_id"])
        res_dict["question"].append(d["question"])
        res_dict["difficulty"].append(d["difficulty"])
        res_dict["products"].append(list(set(prods)))


    df = pd.DataFrame(res_dict)
    df.to_csv("bird_minidev_questions.csv",index=False)
