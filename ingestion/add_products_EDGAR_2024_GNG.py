import yaml
from pathlib import Path
import pandas as pd
import json

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

if __name__ == "__main__":
    data = "../data/data_products/GHG_by_sector_and_country_DACH.csv"
    metadata = "../data/data_products/metadata_manual.json"
    addProduct(data,metadata)