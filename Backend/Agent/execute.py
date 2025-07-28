import ast
import json
import pandas as pd
import requests
import io


def getData(func_dict):
    url = func_dict["product"]
    if "column" in func_dict:
        columns = func_dict["column"]
    else: columns = None


    response = requests.get(url)
    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    if columns is not None:
        try:
            df = df[columns]
        except KeyError:
            df
    return df


def _getDataProduct(url):
    """
    :param agent_result:
    :return:
    """
    try:
        return getData(url)
    except:
        return "could not access data product is the URL correct ?"


# TODO manage when data is to big
def _putDataProduct(df, function):
    if 'values' in function:
        args = json.dumps(function['values'])
    if 'filter_dict' in function:
        args = json.dumps(function['filter_dict'])
    if 'columns' in function:
        args = json.dumps(function['columns'])
    response = requests.put(function["function"],
                            json={"data": df.to_json(), "args": args})

    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    return df

def _putDataProductCombination(first,second, function):

    response = requests.put('http://127.0.0.1:5200/combine',
                            json={"data_1": first.to_json(),"data_2": second.to_json() ,"args": json.dumps(function)})

    content = json.loads(response.text)

    try:
        df = pd.read_json(io.StringIO(content['data']))
    except ValueError:
        df = pd.Series(ast.literal_eval(content['data']))

    return df


def _executeProcessing(df, plan):
    for elem in plan:
        df = _putDataProduct(df, elem)
    return df

def executeStep(plan):

    retrieve = plan[0]
    df = getData(retrieve['values']['product'],retrieve['values']['columns'])

    for elem in plan[1:]:
        df = _putDataProduct(df, elem)

    return df


def execute(agent_result):
    plan = agent_result['products']
    combination = agent_result['combination']
    frames = {}

    for i in range(len(plan)):
        df = _getDataProduct(plan[i]["product"])
        if isinstance(df,str):
            return None
        df = _executeProcessing(df, plan[i]["transformation"])
        frames["df_" + str(i)] = df

    if len(plan) -1 != len(combination): #& len(combination[0]) != 0:
        return frames

    previous = frames["df_0"]
    for i in range(len(combination)):
        new = frames["df_"+str(i+1)]
        previous = _putDataProductCombination(previous,new,combination[i])

    return previous



def execute_new(agent_result):
    plans = agent_result['plans']

    frames = {}
    i = -1
    for elem in plans:
        print(elem)
        if elem['function'] == 'http://127.0.0.1:5200/retrieve':
            df = getData(elem['filter_dict'])
            i += 1
            frames["df_" + str(i)] = df
        elif elem['function'] == "combination":
            previous = frames["df_" + str(i - 1)]
            new = frames["df_" + str(i)]
            df = _putDataProductCombination(previous, new, elem['filter_dict'])

            i += 1
            frames["df_" + str(i)] = df

        else:
            df = _putDataProduct(df, elem)
            frames["df_" + str(i)] = df
        #print(frames["df_" + str(i)])
        try:
            tmp = frames["df_" + str(i)]
            print(tmp["DisplayName"].unique())
        except:
            pass

    return frames["df_" + str(i)]

if __name__ == "__main__":
    plan = {"plans":[[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/superhero/superhero"}},{"function":"http://127.0.0.1:5200/filter","values":{"conditions":{"full_name":"Karen Beecher-Duncan"}}}],[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/superhero/colour"}}]] ,"combination":[{"columns_left":"eye_colour_id","columns_right":"id","type":"equals","values":["None"]} ] }
    plan = {"plans": [[{"function": "http://127.0.0.1:5200/retrieve",
                 "values": {"product": "http://127.0.0.1:5000/products/superhero/superhero", }},
                {"function": "http://127.0.0.1:5200/filter", "values": {"conditions": {"superhero_name": "Copycat"}}}],
               [{"function": "http://127.0.0.1:5200/retrieve",
                 "values": {"product": "http://127.0.0.1:5000/products/superhero/race", }},
                {"function": "http://127.0.0.1:5200/filter", "values": {"conditions": {"superhero_name": "Copycat"}}}]],
     "combination": [{"columns_left": "race_id", "columns_right": "id", "type": "equals", "values": ["None"]}]}
    plan = {"plans":[[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/formula_1/qualifying"}},{"function":"http://127.0.0.1:5200/filter","values":{"conditions":{"raceId":45,"q3":{"max":"1:34","min":"1:33"}}}}],[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/formula_1/drivers"}}]],"combination": [{"columns_left": "driverId", "columns_right": "driverId", "type": "equals", "values": ["None"]}]}
    plan = {"plans": [[{"function": "http://127.0.0.1:5200/retrieve", "values": {"product": "http://127.0.0.1:5000/products/formula_1/races"}},{"function": "http://127.0.0.1:5200/filter", "values": {"raceId": 901}}],[{"function": "http://127.0.0.1:5200/retrieve","values":{"product": "http://127.0.0.1:5000/products/formula_1/seasons"}}]],"combination":[{"columns_left": "year", "columns_right": "year", "type": "equals", "values": ["None"]}]}
    plan = {"plans": [[{"function": "http://127.0.0.1:5200/retrieve","values": {"product": "http://127.0.0.1:5000/products/formula_1/drivers"}}],[{"function": "http://127.0.0.1:5200/retrieve","values": {"product": "http://127.0.0.1:5000/products/formula_1/results"}},{"function": "http://127.0.0.1:5200/sum", "values": {"group_by": "driverId", "column": "points"}},{"function": "http://127.0.0.1:5200/max", "values": {"columns": "driverId", "rows": 1}}]], "combination": [{"columns_left": "driverId", "columns_right": "driverId", "type": "equals", "values": ["None"]}]}
    plan = {"plans":[[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/formula_1/drivers"}},{"function": "http://127.0.0.1:5200/filter", "values": {"conditions": {"forename": "Lewis","surname": "Hamilton"}}}],[{"function": "http://127.0.0.1:5200/retrieve","values": {"product": "http://127.0.0.1:5000/products/formula_1/results"}}],[{"function": "http://127.0.0.1:5200/retrieve", "values": {"product": "http://127.0.0.1:5000/products/formula_1/races"}},{"function": "http://127.0.0.1:5200/filter", "values": {"conditions": {"year": "2008","name": "Chinese Grand Prix"}}}]],"combination": [{"columns_left": "driverId", "columns_right": "driverId", "type": "equals", "values": ["None"]},{"columns_left": "raceId", "columns_right": "raceId", "type": "equals", "values": ["None"]}]}

    #TODO need aggregation column
    plan = {"plans":[[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/formula_1/races"}},{"function":"http://127.0.0.1:5200/filter","values":{"conditions":{"name":"Bahrain Grand Prix","year":2007}}}],[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/formula_1/results"}},{"function":"http://127.0.0.1:5200/filter","values":{"conditions":{"time":"empty"}}}]],"combination": [{"columns_left": "raceId", "columns_right": "raceId", "type": "equals", "values": ["None"]}]}

    plan = {"plans":[[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/card_games/sets"}},{"function":"http://127.0.0.1:5200/filter","values":{"conditions":{"name":"Eighth Edition"}}}],[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/card_games/set_translations"}},{"function":"http://127.0.0.1:5200/filter","values":{"conditions":{"language":"Chinese Simplified"}}}]],"combination": [{"columns_left": "code", "columns_right": "setCode", "type": "equals", "values": ["None"]}]}
    plan = {"plans":[[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/european_football_2/Player"}},{"function":"http://127.0.0.1:5200/filter","values":{"conditions":{"player_name":"Pietro Marino"}}}],[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/european_football_2/Player_Attributes"}},{"function":"http://127.0.0.1:5200/mean","values":{"group_by":["player_api_id"],"columns":["overall_rating"]}}]],"combination": [{"columns_left": "player_api_id", "columns_right": "player_api_id", "type": "equals", "values": ["None"]}]}
    plan = {"plans":[[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/student_club/Attendance"}},{"function":"http://127.0.0.1:5200/count","values":{"group_by":["link_to_member"],"columns":["link_to_member"]}},{"function":"http://127.0.0.1:5200/filter","values":{"conditions":{"count":{"min":7}}}}],[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/student_club/Member"}}]],"combination": [{"columns_left": "link_to_member", "columns_right": "member_id", "type": "equals", "values": ["None"]}]}
    plan = {"plans":[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/student_club/Income"}},{"function":"http://127.0.0.1:5200/filter","values":{"date_received":"2019-09-14","source":"Fundraising"}}],"combination":[]}
    plan = {"plans":[[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/european_football_2/Match"}},{"function":"http://127.0.0.1:5200/filter","values":{"season":"2015/2016"}},{"function":"http://127.0.0.1:5200/count","values":{"group_by":["league_id"],"columns":["league_id"]}},{"function":"http://127.0.0.1:5200/max","values":{"rows":4,"columns":["count"]}}],[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/european_football_2/League"}}]],"combination": [{"columns_left": "league_id", "columns_right": "id", "type": "equals", "values": ["None"]}]}
    plan = {"plans":[[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/european_football_2/Player_Attributes"}},{"function":"http://127.0.0.1:5200/max","values":{"column":"overall_rating","rows":1}}],[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/european_football_2/Player"}}]],"combination": [{"columns_left": "player_api_id", "columns_right": "player_api_id", "type": "equals", "values": ["None"]}]}
    plan = {"plans":[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/card_games/cards"}},{"function":"http://127.0.0.1:5200/filter","values":{"conditions":{"cardKingdomFoilId":"not empty","cardKingdomId":"not empty"}}}]}
    plan= {"plans":[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/card_games/cards"}},{"function":"http://127.0.0.1:5200/filter","values":{"conditions":{"cardKingdomFoilId":"not empty","cardKingdomId":"not empty","borderColor":"borderless"}}}]}

    plan = {"plans":[[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/superhero/superhero"}}],[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/superhero/gender"}},{"function":"http://127.0.0.1:5200/filter","values":{"conditions":{"gender":"male"}}}],[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/superhero/hero_power"}}],[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/superhero/superpower"}}]],"combination": [{"columns_left": "gender_id", "columns_right": "id", "type": "equals", "values": ["None"]},{"columns_left": "id", "columns_right": "hero_id", "type": "equals", "values": ["None"]},{"columns_left": "power_id", "columns_right": "id", "type": "equals", "values": ["None"]}]}
    plan = {"plans":[[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/european_football_2/Match"}},{"function":"http://127.0.0.1:5200/count","values":{"group_by":["league_id"],"column":"league_id"}},{"function":"http://127.0.0.1:5200/max","values":{"column":"count","rows":1}}],[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/european_football_2/League"}}]],"combination": [{"columns_left": "league_id", "columns_right": "id", "type": "equals", "values": ["None"]}]}

    #plan = {"plans":[[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/student_club/Attendance"}},{"function":"http://127.0.0.1:5200/count","values":{"group_by":["link_to_member"],"columns":["link_to_member"]}},{"function":"http://127.0.0.1:5200/filter","values":{"conditions":{"count":{"min":7}}}}],[{"function":"http://127.0.0.1:5200/retrieve","values":{"product":"http://127.0.0.1:5000/products/student_club/Member"}}]],"combination": [{"columns_left": "link_to_member", "columns_right": "member_id", "type": "equals", "values": ["None"]}]}
    l = {'plans': [{'function': 'http://127.0.0.1:5200/retrieve',
                    'filter_dict': {'product': 'http://127.0.0.1:5000/products/superhero/superhero'}},
                   {"function": "http://127.0.0.1:5200/filter", "values": {"conditions": {"superhero_name": "Copycat"}}},
                   {'function': 'http://127.0.0.1:5200/retrieve',
                    'filter_dict': {'product': 'http://127.0.0.1:5000/products/superhero/race'}}, {"function":
                       'combination', 'filter_dict':{'columns_left': 'race_id', 'columns_right': 'id', 'type': 'equals',
                                       'values': ['None']}},
                   {"function": "http://127.0.0.1:5200/sum", "values": {"group_by": "superhero_name", "column": "weight_kg"}}]}


    wip = {'plans': [
{'function': 'http://127.0.0.1:5200/retrieve','filter_dict': {'product': 'http://127.0.0.1:5000/products/financial/district'}},
{'function': 'http://127.0.0.1:5200/filter', "values": {"conditions": {"A11":{"min":8000,"max":9000}}}},
{'function': 'http://127.0.0.1:5200/retrieve','filter_dict': {'product': 'http://127.0.0.1:5000/products/financial/account'}},
{'function':'combination', 'filter_dict':{'columns_left': 'district_id', 'columns_right': 'district_id', 'type': 'equals','values': ['None']}},
{'function': 'http://127.0.0.1:5200/retrieve','filter_dict': {'product': 'http://127.0.0.1:5000/products/financial/disp'}},
{'function': 'http://127.0.0.1:5200/filter', "values": {"conditions": {"type":"DISPONENT"}}},
{'function':'combination', 'filter_dict':{'columns_left': 'account_id', 'columns_right': 'account_id', 'type': 'equals','values': ['None']}}]}

    l ={'plans': [{'function': 'http://127.0.0.1:5200/retrieve','filter_dict': {'product': 'http://127.0.0.1:5000/products/formula_1/drivers'}},{'function': 'http://127.0.0.1:5200/filter', "values": {"conditions": {"nationality":"German", "dob":{"min":"1980-01-01", "max": "1985-12-31"}}}},{'function': 'http://127.0.0.1:5200/retrieve','filter_dict': {'product': 'http://127.0.0.1:5000/products/formula_1/pitStops'}},{"function":"http://127.0.0.1:5200/mean","filter_dict":{"columns":"milliseconds","group_by":"driverId"}},{'function':'combination', 'filter_dict':{'columns_left': 'driverId', 'columns_right': 'driverId', 'type': 'equals','values': ['None']}},{"function":"http://127.0.0.1:5200/min","filter_dict":{"column":"milliseconds","rows":3}}]}








    print("result",execute_new(l))