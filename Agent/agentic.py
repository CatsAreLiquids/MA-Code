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


# user = "The Co2 data for Arubas building sector where the emissions are between 0.02 and 0.03 "
# user= "Emissions Data for Austrias Argiculture and building Sector for the 1990er"
# user = "Sweden, Norveigen and Finnlands per capita Co2 emissions "
user = "All females customers who paid with Credit Card and are at least 38 years old"
# user= "Germanies emisson for the 2000s"

ragent = retrieval_agent.init_agent()
pagent = processing_agent.init_agent()

agent_result = ragent.invoke({"input": user})['output']
agent_result = json.loads(agent_result)
if ('name' in agent_result) and ('url' in agent_result):
    df = util.getData(agent_result['url'])
    user = user + f"The correct data products name is {agent_result['name']}"
agent_result = pagent.invoke({"input": user})['output']
print(agent_result)
try:
    agent_result = ast.literal_eval(agent_result)
    for elem in agent_result:
        print(elem)
        if 'values' in elem:
            values = elem['values']
        else:
            values = None
        df = applyFunction(df, elem['function'], values)
except:
    pass

# JSONDecodeError
print(df)
