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
        with open("../data/Catalogs/transformation_catalog.yml") as stream:
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


def _add_product(productName: str, description:dict, collection):
    vector_store = models.getVectorStore()

    meta_dict = {"name": productName,
                "type": "products",
                 "collection":collection,
                 "id": str(uuid.uuid4())}

    doc = Document(page_content=description["description"], metadata=meta_dict)

    vector_store.add_documents([doc])


def add_products(collection, productName: str | None):
    try:
        with open(f"../data/Catalogs/{collection}.yml") as stream:
            data = yaml.safe_load(stream)
        if productName is not None:
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

def delete(entry_id: List | None, collection=None):
    vector_store = models.getVectorStore(collection)

    if entry_id is None:
        vector_store.delete_collection()
    else:
        vector_store.delete(entry_id)

if __name__ == "__main__":
    pass

