import os
from dotenv import load_dotenv
import json
import requests
from datetime import date
import requests
import numpy as np
import pandas as pd
from pydantic import BaseModel, Field
import re
import yaml
import io


# TODO need to keep the titles of the data
def parseResult(str):
    urls = re.findall(r"\]\s*\((.*?)\)", str)
    titles = re.findall(r"\[(.*?)\]", str)

    if urls:
        return {'success': True, 'urls': urls, 'data_names': titles, 'message': 'I found your data'}
    else:
        return {'success': False, 'urls': None, 'data_names': None, 'message': str}


# Note works since we currently only use the products string needs adjustemnt if I extend API
def validateURL(urls):
    valid = []

    for url in urls:
        if "http://127.0.0.1:5000/" in url:

            parsedProduct = url.split("products/")[1].split("/")[0]

            if parsedProduct in productList:
                valid.append(url)

    if not valid:
        return ("No valid Urls found please try rephrasing your question", [])
    elif len(valid) != len(urls):
        return ("Some generated Urls are invalid please review the data and try rephrasing", valid)
    else:
        return ("I found the data you were looking for please wait while I load it ", valid)


def parseRetrieval():
    pass


def getData(url):
    response = requests.get(url)
    content = json.loads(response.text)
    return pd.read_json(io.StringIO(content['data']))
