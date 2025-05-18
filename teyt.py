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

def _getDataProduct(agent_result):
    """
    :param agent_result:
    :return:
    """
    try:
        return util.getData(agent_result['url'])
    except:
        return "could not access data product is the URL correct ?"

def applyFunction(df, function, values):
    if function in aggregations:
        return aggregations[function](df, values)
    if function in filters:
        return filters[function](df, values)

    return df

def _executeBlocks(df,plan):
    try:
        plan = ast.literal_eval(plan)
        for elem in plan:
            if 'values' in elem:
                values = elem['values']
            else:
                values = None
            df = applyFunction(df, elem['function'], values)
    except:
        pass

    return df

def execute(plan):

    if 'combine' in plan:
        df = _getDataProduct(plan['combine']['p1'][0])
        df = _executeBlocks(df, plan['combine']['p1'][1])

        df2 = _getDataProduct(plan['combine']['p2'][0])
        df2 = _executeBlocks(df2, plan['combine']['p2'][1])

        df = _combineProducts(df,df2,plan['column'],plan['type'],plan['values'])

    else:
        df = _getDataProduct(plan['execute']['p1'][0])
        df = _executeBlocks(df,plan['execute']['p1'][1])

    return df

def _combineProducts(first,second, column,type,value):

    if type =="select":
        first = first[first[column].isin(value)]
    if type == "join":
        first = first.join(second,on=column)
        if value is not None:
            first = first[first[column].isin(value)]
    return first


def mean(df,filter_dict):
    if "group_by" in filter_dict:
        df = df.groupby(by=filter_dict['group_by'])

    return df[filter_dict['column']].mean()

l = {"combine":{"p1":({"name": "sales_data_23", "url": "http://127.0.0.1:5000/products/Sales_Data/sales_data_23"},[{"function":"sum","values":{"column":"price","group_by":["category"]}}]),"p2":({"name": "customer_data_23", "url": "http://127.0.0.1:5000/products/Sales_Data/customer_data_23"},[{"function":"filter","values":{"gender":"Women","age":{"min":38}}}])},"column":"customer_id","type":"select","values":["None"]}

#agent_result = json.loads(l)
#execute(agent_result)
df = pd.read_csv("Backend/data/Sales_Data/sales_data_23.csv")
df = df[df['category']=='toys'].groupby(["shopping_mall"])['quantity'].sum()
file = 'http://127.0.0.1:5000/products/Sales_Data/customer_data_23'
if 'http' in file:
    file = file.split("/")[-1]

print(file)
print(len({}))

#df = pd.read_csv("GHG_per_GDP_by_country.csv")
file = "Sales_Data/customer_data_23"
if "/" in file:
    file = file.split("/")[-1]
print(file)