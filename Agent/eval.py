from MultiAgentSystem import runQuery
import time
from transformations.execute import execute
import pandas as pd
import ast

def q1():
    customers = pd.read_csv("../data/Sales_Data/customer_data_23.csv")
    sales = pd.read_csv("../data/Sales_Data/sales_data_23.csv")

    customers = customers[customers['gender'] == 'female']
    customers = customers[customers['age'] >= 38]

    sales = sales.merge(customers, on=['customer_id'])

    return sales.groupby(['category'])['price'].sum()


def compareResult():
    pass


def evalAgent():
    # user = "The Co2 data for Arubas building sector where the emissions are between 0.02 and 0.03 "
    # user= "Emissions Data for Austrias Argiculture and building Sector for the 1990er"
    # user = "Sweden, Norveigen and Finnlands per capita Co2 emissions "
    user = "All females customers who paid with Credit Card and are at least 38 years old"
    user = "From the sales data i would like to know the total amount of money spent per category of available items, of Females over 38"
    # user= "Germanies emisson for the 2000s"

    start = time.time()
    agent_result, callbacks = runQuery(user)
    agent_result= ast.literal_eval(agent_result['output'])

    end = time.time()
    query_result = execute(agent_result)
    ground_truth = q1()
    print("Time:", end - start)
    print(callbacks.usage_metadata)
    print((ground_truth==query_result).all())


if __name__ == "__main__":
    evalAgent()
