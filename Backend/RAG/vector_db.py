import json
import uuid
from typing import List

import yaml
from dotenv import load_dotenv
from langchain_core.documents import Document

from Backend import models

load_dotenv()


def _add_transformation(functionName: str, description: dict):
    vector_store = models.getVectorStore()

    text = f"function name:{functionName}\n"
    for k, v in description.items():
        text += f"{k}: {v}\n"

    doc = [Document(page_content=text, metadata={"type": "function", "id": str(uuid.uuid4()),"name":functionName})]
    vector_store.add_documents(doc)

def add_transformations(functionName: str | None):
    try:
        with open("../data/Catalogs/function_catalog.yml") as stream:
            data = yaml.safe_load(stream)
            if functionName is not None:
                _add_transformation(functionName, data[functionName])
                return
    except FileNotFoundError:
        print("Could not find the file catalog at data/Catalogs/ . Wont proceed")
        return
    except KeyError:
        print("Could not find the specified function in the catalog. Wont proceed")
        return

    for k, v in data.items():
        _add_transformation(k, v)


def _add_product(productName: str, description: dict, collection):
    vector_store = models.getVectorStore()

    tags = description["tags"]
    meta_dict = {"tags": tags,
                 "type": "product",
                 "collection":collection,
                 "id": str(uuid.uuid4())}

    doc = Document(page_content=description["description"], metadata=meta_dict)
    vector_store.add_documents([doc])


def add_products(collection, productName: str | None):
    try:
        data = json.load(open(f"../data/{collection}/metadata_automatic.json"))
        if productName is not None:
            print(data[productName])
            _add_product(productName, data[productName], collection)
            return
    except FileNotFoundError:
        print(f"Could not find the product describtions at /data/{collection}/. Wont proceed")
        return
    except KeyError:
        print("Could not find the specified product in the catalog. Wont proceed")
        return

    for k, v in data.items():
        _add_product(k, v, collection)

def _add_collection(collection_name, description):
    vector_store = models.getVectorStore("collection_level")

    meta_dict = {"name": collection_name,
                 "id": str(uuid.uuid4())}

    doc = Document(page_content=description, metadata=meta_dict)
    vector_store.add_documents([doc])


def add_collections(collection_name=None):
    try:
        with open("../data/Catalogs/catalog.yml") as stream:
            catalog = yaml.safe_load(stream)
            if collection_name is not None:
                _add_collection(catalog[collection_name]["name"], catalog[collection_name]["description"])
    except FileNotFoundError:
        print("could not find the main catalog")
        return
    except KeyError:
        print("Could not find the collection. Wont proceed")
        return

    for k, v in catalog.items():
        _add_collection(catalog[k]["name"], catalog[k]["description"])

def delete(id: List | None, collection=None):
    vector_store = models.getVectorStore(collection)

    if id is None:
        vector_store.delete_collection()
    else:
        vector_store.delete(id)

if __name__ == "__main__":
    # add_collections()
    db = models.getVectorStore()
    #add_Functions_Test(None)
    #print(rephrase_query("retrieve drivers dataset","State code numbers of top 3 yougest drivers. How many Netherlandic drivers among them?"))
    #add_collections()


    prompt = """Your task is to help find the best fitting data product. You are provided with a user query .
                    Provide the most likely fitting data products, always provide the data products name and why it would fit this query
            Question: {question} 
            Context: {context} 
            Answer:
        """
    #multiLevelChain("What is the ratio of customers who pay in EUR against customers who pay in CZK?", prompt,
    #                {"filter": {"type": {"$eq": "product"}}})

    #add_Functions("sortby")
    #delete(id= ["93e6e754-d8d9-4e1b-9bd7-24da841c0644","c4a436be-0190-4d80-867e-ec18f0e821c6"],collection=None)


    get_docs_score(query = "",max= 100,filter={"type": {"$eq": 'function_NoManual'}})#,filter={"type": {"$eq": "product"}}#,filter={"type": {"$eq": "function_NoManual"}}
    #add_Functions("retrieve")
    #add_docs("toxicology",None)
    #evalEmbbeds("")
    #stuff()
