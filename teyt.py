import pandas as pd
import re
import requests
import json
import io
import numpy as np
from uuid import uuid4
import langchain_openai
import yaml

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
    previous_df = None
    for product,processing in plan():
        df = _getDataProduct(product)
        df = _executeBlocks(df,processing)
        if previous_df is not None and "combineProducts" in processing:
            ecombineProducts




r1= {'name': 'sales_data_23', 'url': 'http://127.0.0.1:5000/products/Sales_Data/sales_data_23'}
r2 = {'name': 'customer_data_23', 'url': 'http://127.0.0.1:5000/products/Sales_Data/customer_data_23'}
one = [{"function":"filter","values":{"gender":"Female","age":{"min":38}}},{"function":"getRows","values":{"customer_id":"None"}}]
two = [{"function":"combineProducts","values":{"customer_id":["C109593"]}},{"function":"sum","values":{"group_by":"category"}}]

plan = [(r1,one),(r2,two)]


