from MultiAgentSystem import runQuery
import time
from transformations.execute import execute
import pandas as pd
import numpy as np
import ast

def q1():
    # From the sales data i would like to know the total amount of money spent per category of available items, of Females over 38
    customers = pd.read_csv("../data/Sales_Data/customer_data_23.csv")
    sales = pd.read_csv("../data/Sales_Data/sales_data_23.csv")

    customers = customers[customers['gender'] == 'female']
    customers = customers[customers['age'] >= 38]

    sales = sales.merge(customers, on=['customer_id'])

    return sales.groupby(['category'])['price'].sum()


def q2():
    # Germanies total emissons for the 2000s
    df = pd.read_csv("../data/EDGAR_2024_GHG/GHG_totals_by_country.csv")
    df = df[df['Country'] == 'Germany']
    df = df[['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009']]

    return df


def q3():
    # All customers who shopped in the mall with the highest turnover
    customers = pd.read_csv("../data/Sales_Data/customer_data_23.csv")
    sales = pd.read_csv("../data/Sales_Data/sales_data_23.csv")
    tmp = sales.groupby(['shopping_mall'])['price'].sum()
    highest = tmp.sort_values(ascending=False).keys().tolist()[0]

    sales = sales[sales['shopping_mall'] == highest]

    return customers.merge(sales['customer_id'], on=['customer_id'], how='inner')


def q4():
    # "Sweden, Norveigen and Finnlands per capita Co2 emissions"
    df = pd.read_csv("../data/EDGAR_2024_GHG/GHG_per_capita_by_country.csv")
    return df[df['Country'].isin(['Sweden', 'Norway', 'Finland'])]


def q5():
    # "The ten countries with the highes per GDP emissions for 2007"
    df = pd.read_csv("../data/EDGAR_2024_GHG/GHG_per_GDP_by_country.csv")
    df = df[['2007', 'Country']].sort_values(ascending=False, by=['2007'])

    return df[:10]


def q6():
    # "The Co2 data for Arubas building sector where the emissions are between 0.02 and 0.03 "

    df = pd.read_csv("../data/EDGAR_2024_GHG/GHG_by_sector_and_country.csv")
    mask = (df['Country'] == 'Aruba') & (df['Substance'] == 'CO2') & (df['Sector'] == 'Buildings')
    df = df[mask]
    mask = np.where((0.02 < df.T[4:]) & (df.T[4:] < 0.03))[0] + 4

    return df.T.iloc[mask]

def q7():
    # The GWP_100_AR5_GHG data per region and sector decrasing
    df = pd.read_csv("../data/EDGAR_2024_GHG/LULUCF_macroregions.csv")
    df = df[df['Substance'] == 'GWP_100_AR5_GHG']
    return df.max().to_frame().T


def evalAgent():
    queries = {
        'q1': {'function': q1, 'query': "From the sales data i would like to know the total amount of money spent per "
                                        "category of available items, of Females over 38"},
        'q2': {'function': q2, 'query': "Germanies total emissons for the 2000s"},
        'q3': {'function': q3, 'query': "All customers data for customers who shopped in the mall with the highest turnover"},
        'q4': {'function': q4, 'query': "Sweden, Norveigen and Finnlands per capita Co2 emissions"},
        'q5': {'function': q5, 'query': "The ten countries ith the highes per capita emissions for 2007"},
        'q6': {'function': q6, 'query': "The Co2 data for Arubas building sector where the emissions are between 0.02 "
                                        "and 0.03 "},
        'q7': {'function': q7, 'query': "The GWP_100_AR5_GHG data per region and sector for each year"}, }

    start = time.time()
    agent_result, callbacks = runQuery(queries['q4']['query'])
    agent_result= ast.literal_eval(agent_result['output'])
    end = time.time()
    query_result = execute(agent_result)
    print(agent_result)

    ground_truth = queries['q']['function']()
    print("Time:", end - start)
    print(callbacks.usage_metadata)
    try: print((ground_truth == query_result).all())
    except ValueError:
        print(ground_truth)
        print(query_result)


if __name__ == "__main__":
    evalAgent()

