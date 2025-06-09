import pandas as pd
import re
import requests
import json
import io
import numpy as np
from uuid import uuid4
import langchain_openai
import yaml
import ast


def getCatalog(file):
    if "/" in file:
        file = file.split("/")[-1]

    try:
        with open("Backend/data/Catalogs/catalog.yml") as stream:
            catalog = yaml.safe_load(stream)
    except FileNotFoundError:
        return "could not find the main catalog"
    for collection in catalog:
        collection =  catalog[collection]
        if file in collection['products']:
            try:
                with open("Backend/data/Catalogs/" + collection['name'] + ".yml") as stream:
                    collection_dict = yaml.safe_load(stream)
                    print(file)
                    return collection_dict[file]
            except FileNotFoundError:
                return "could not find the specific collection catalog"
            except KeyError:
                return collection_dict

print(getCatalog('customers'))