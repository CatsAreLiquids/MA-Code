import yaml
from pathlib import Path
import pandas as pd
import json
from langchain_core.documents import Document

import stores




config_name = "../config/EDGAR_2024_GNG_config.yml"

def tmp(meta_dict,schema):
    pass



def CreateEntities(schema,data):
    pass

def addMetadata(meta_json):
    with open("../config/config.yml", 'r') as f:
        general_schema= yaml.load(f,Loader=yaml.FullLoader)
    print(meta_json)
    print(general_schema)


def addData(data):

    graph = stores.load_graph_db()
    with open(config_name, 'r') as f:
        specific_schema = yaml.load(f,Loader=yaml.FullLoader)
    print(specific_schema)


    pass

def addProduct(data, metadata):
    df = pd.read_csv(data)
    filename = Path(data).stem

    mdata = json.load(open(metadata))

    with open("../config/config.yml", 'r') as f:
        general_schema= yaml.load(f,Loader=yaml.FullLoader)

    meta_dict = {"tags": mdata["tags"],
                 "type": mdata["type"],
                 'file':filename,
                 #TODO make this variable
                 "min_year": mdata["min_year"],
                 "max_year": mdata["max_year"]}
    graphDoc = Document(page_content=mdata["description"], metadata=meta_dict)



    addData(df)
    #addMetadata(mdata[filename])

    return ""

def add_doc(file, type):
    db = stores.load_vector_store()
    data = json.load(open(file))
    docs = []
    for key in data.keys():
        tags = data[key]["tags"]
        tags2 = ','.join(tags)
        meta_dict = {"tags": tags, "tags2": tags2, "type": type, "file": key, "min_year": data[key]["min_year"],
                     "max_year": data[key]["max_year"]}
        docs.append(Document(page_content=data[key]["description"], metadata=meta_dict))
        print(key)
    db.add_documents(docs)

if __name__ == "__main__":
    metadata = "../data/data_products/EDGAR_2024_GHG/metadata_automatic.json"
    db = stores.load_vector_store()
    db.delete_collection()
    add_doc(metadata,"automatic")

    db.similarity_search("Sum of Germanys emission form the 1990's onward")