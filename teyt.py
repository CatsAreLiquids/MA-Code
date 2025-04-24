import pandas as pd
import re
import requests
import json
import io
import numpy as np
from uuid import uuid4
productList =['EDGAR_2024_GHG']
def parseResult(str):
    urls = re.findall(r"\]\s*\((.*?)\)", str)
    titles = re.findall(r"\[(.*?)\]", str)

    if urls:
        return (True, urls,titles)
    else:
        return (False, str)

def validateURL(urls):
    valid= []

    for url in urls:
        if "http://127.0.0.1:5000/" in url:

            parsedProduct = url.split("products/")[1].split("/")[0]

            if parsedProduct in productList:
                valid.append(url)

    if not valid:
        return ("No valid Urls found please try rephrasing your question",[])
    elif len(valid) != len(urls):
        return ("Some generated Urls are invalid please review the data and try rephrasing",valid)
    else:
        return ("True",valid)

def parseData(urls):
    content = []

    url = urls.pop()
    response = requests.get(url)
    content = json.loads(response.text)
    df = pd.read_json(io.StringIO(content['data']))

    for url in urls:
        response = requests.get(url)
        content = json.loads(response.text)
        df_to_merge = pd.read_json(io.StringIO(content['data']))
        #try:
        df = pd.concat([df, df_to_merge], axis=1, join="inner")
        #except:
        #    fallBack(df,urls)
        #    return None
    return df

txt = """
 - [Building Sector CO2 Emissions](http://127.0.0.1:5000/products/EDGAR_2024_GHG/filter?file=GHG_by_sector_and_country_CO2_Buildings&func=None&Country1=Germany)
"""


def getSum(df, rolling, period):
    if period not in ["none", "None"]:
        print(np.asarray(df))
        arr = np.asarray(df).reshape(-1,period)
        print(arr)
        return arr.sum(axis=0)
    return df.sum().reset_index()

#val,url,titles = parseResult(txt)
#mes, urls =validateURL(url)
#df = parseData(urls)
#df = df.sum().reset_index()
#print(df)

tmp = {'col1': [1, 2,5,6], 'col2': [4, 3,1,4]}
min = "2"
max = "4"
df = pd.DataFrame.from_dict(tmp)
print(df)
print(getSum(df,None,2))

print(uuid4())

