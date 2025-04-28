from Agent.agents import processing_agent, retrieval_agent
import util
import json
import ast

# from transformations.aggregation import func_dict as aggregations
# from transformations.filter import func_dict as filters
from transformations import aggregation
from transformations import filter

aggregations = {'sum': aggregation.getSum, "mean": aggregation.mean}
filters = {'filter': filter.placeholder}


def applyFunction(df, function, values):
    if function in aggregations:
        return aggregations[function](df, values)
    if function in filters:
        return filters[function](df, values)

    return df

agent_result = [{"function":"filter","values":{"gender":"Female","min_age":38,"payment_method":"Credit Card"}},{"function":"sum","values":{}}]
url = "http://127.0.0.1:5000/products/Sales_Data/customer_data_23"
df = util.getData(url)
print(df)
#agent_result = ast.literal_eval(test)
for elem in agent_result:
    print(elem)
    if 'values' in elem:
        values = elem['values']
    else:
        values = None
    df = applyFunction(df, elem['function'], values)

print(df)
