import pandas as pd
import requests
import json
import io
import time
from Backend.Agent import execute

def _get_df(product, collection):
    response = requests.get(f"http://127.0.0.1:5000/products/{collection}/{product}")
    content = json.loads(response.text)
    df = pd.read_json(io.StringIO(content['data']))
    return df


def q_468():
    df1 = _get_df("sets", "card_games")
    df2 = _get_df("set_translations", "card_games")

    df1 = df1[df1["name"] == "Eighth Edition"]
    df2 = df2[df2["language"] == "Chinese Simplified"]
    res = df1.merge(df2, suffixes=["", "_y"], left_on="code", right_on='setCode')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)
    res.to_csv("results/sql_result_468.csv",index=False)



def q_340():
    df1 = _get_df("cards", "card_games")
    df1 = df1.dropna(how="any", subset=["cardKingdomFoilId", "cardKingdomId"])
    df1.to_csv("results/sql_result_340.csv",index=False)


def q_341():
    df1 = _get_df("cards", "card_games")
    df1 = df1.dropna(how="any", subset=["cardKingdomFoilId", "cardKingdomId"])
    df1 = df1[df1["borderColor"] == "borderless"]
    df1.to_csv("results/sql_result_341.csv",index=False)


def q_806():
    df1 = _get_df("superhero", "superhero")
    df1 = df1[df1["full_name"] == "Karen Beecher-Duncan"]

    df2 = _get_df("colour", "superhero")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="eye_colour_id", right_on='id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)
    res.to_csv("results/sql_result_806.csv",index=False)


def q_737():
    df1 = _get_df("superhero", "superhero")
    df1 = df1[df1["superhero_name"] == "Copycat"]

    df2 = _get_df("race", "superhero")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="race_id", right_on='id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)
    res.to_csv("results/sql_result_737.csv",index=False)


def q_751():
    df1 = _get_df("superhero", "superhero")

    df2 = _get_df("gender", "superhero")
    df2 = df2[df2["gender"] == "Male"]
    res = df1.merge(df2, suffixes=["", "_y"], left_on="gender_id", right_on='id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    df3 = _get_df("hero_power", "superhero")
    res = res.merge(df3, suffixes=["", "_y"], left_on="id", right_on='hero_id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    df4 = _get_df("superpower", "superhero")
    res = res.merge(df4, suffixes=["", "_y"], left_on="power_id", right_on='id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res.to_csv("results/sql_result_751.csv",index=False)


def q_872():
    df1 = _get_df("qualifying", "formula_1")
    df1 = df1[df1["raceId"] == 45]
    df1 = df1[df1["q3"].str.contains("1:33", na=False)]

    df2 = _get_df("drivers", "formula_1")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="driverId", right_on='driverId')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)
    res.to_csv("results/sql_result_872.csv",index=False)


def q_875():
    df1 = _get_df("races", "formula_1")
    df1 = df1[df1["raceId"] == 901]

    df2 = _get_df("seasons", "formula_1")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="year", right_on='year')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)
    res.to_csv("results/sql_result_875.csv",index=False)


def q_933():
    df1 = _get_df("drivers", "formula_1")
    df1 = df1[df1["forename"] == "Lewis"]
    df1 = df1[df1["surname"] == "Hamilton"]

    df2 = _get_df("results", "formula_1")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="driverId", right_on='driverId')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    df3 = _get_df("races", "formula_1")
    df3 = df3[df3["year"] == 2008]
    df3 = df3[df3["name"] == "Chinese Grand Prix"]
    res = res.merge(df3, suffixes=["", "_y"], left_on="raceId", right_on='raceId')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)
    res.to_csv("results/sql_result_933.csv",index=False)


def q_892():
    df1 = _get_df("drivers", "formula_1")
    df2 = _get_df("results", "formula_1")
    df2 = pd.DataFrame(df2.groupby(by=["driverId"]).sum().nlargest(n=1, columns=["points"]))

    res = df1.merge(df2, suffixes=["", "_y"], left_on="driverId", right_on='driverId')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res.to_csv("results/sql_result_892.csv",index=False)


def q_1096():
    df1 = _get_df("Player", "european_football_2")
    df1 = df1[df1["player_name"] == "Pietro Marino"]

    df2 = _get_df("Player_Attributes", "european_football_2")
    df2 = df2.groupby(by=["player_api_id"])["overall_rating"].mean()
    res = df1.merge(df2, suffixes=["", "_y"], left_on="player_api_id", right_on='player_api_id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res.to_csv("results/sql_result_1096.csv",index=False)


def q_1145():
    df1 = _get_df("Match", "european_football_2")
    df1 = df1[df1["season"] == "2015/2016"]
    df1 = df1.groupby(by=["league_id"])["league_id"].agg('count').rename("count").to_frame().reset_index().nlargest(n=4,
                                                                                                                    columns=[
                                                                                                                        "count"])

    df2 = _get_df("League", "european_football_2")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="league_id", right_on='id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res.to_csv("results/sql_result_1145.csv",index=False)


def q_1147():
    df1 = _get_df("Player_Attributes", "european_football_2")
    df1 = df1.nlargest(n=1, columns=["overall_rating"])

    df2 = _get_df("Player", "european_football_2")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="player_api_id", right_on='player_api_id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res.to_csv("results/sql_result_1147.csv",index=False)


def q_1032():
    df1 = _get_df("Match", "european_football_2")
    df1 = df1.groupby(by=["league_id"])["league_id"].agg('count').rename("count").to_frame().reset_index().nlargest(n=1,
                                                                                                                    columns=[
                                                                                                                        "count"])

    df2 = _get_df("League", "european_football_2")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="league_id", right_on='id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res.to_csv("results/sql_result_1032.csv",index=False)


def q_1381():
    df1 = _get_df("Attendance", "student_club")
    df1 = df1.groupby(by=["link_to_member"])["link_to_member"].agg('count').rename("count").to_frame().reset_index()
    df1 = df1[df1["count"] >= 7]

    df2 = _get_df("Member", "student_club")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="link_to_member", right_on='member_id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res.to_csv("results/sql_result_1381.csv",index=False)


def q_1344():
    df1 = _get_df("Income", "student_club")
    df1 = df1[df1["date_received"] == "2019-09-14"]
    df1 = df1[df1["source"] == "Fundraising"]

    df1.to_csv("results/sql_result_1344.csv",index=False)


def q_1405():
    df1 = _get_df("Event", "student_club")
    df1 = df1[df1["event_name"] == "April Speaker"]

    df2 = _get_df("Budget", "student_club")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="event_id", right_on='link_to_event')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res = res.sort_values(by=["amount"])
    res.to_csv("results/sql_result_1405.csv",index=False)


def q_1334():
    df1 = _get_df("Member", "student_club")

    df2 = _get_df("Zip_Code", "student_club")
    df2 = df2[df2["state"] == "Illinois"]
    res = df1.merge(df2, suffixes=["", "_y"], left_on="zip", right_on='zip_code')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res.to_csv("results/sql_result_1334.csv",index=False)


def q_17():
    df1 = _get_df("satscores", "california_schools")
    df1 = df1[df1["AvgScrWrite"] >= 499]

    df2 = _get_df("schools", "california_schools")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="cds", right_on='CDSCode')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res = res.sort_values(by=["AvgScrWrite"])
    res.to_csv("results/sql_result_17.csv",index=False)


def q_39():
    df1 = _get_df("schools", "california_schools")
    df1 = df1[df1["County"] == "Fresno"]
    df1 = df1[(df1["OpenDate"] >= "1980-01-01") & (df1["OpenDate"] <= "1980-12-31")]

    df2 = _get_df("satscores", "california_schools")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="CDSCode", right_on='cds')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res = res["NumTstTakr"].mean()
    res = pd.Series(res)
    res.to_csv("results/sql_result_39.csv",index=False)


def q_578():
    df1 = _get_df("users", "codebase_community")

    df2 = _get_df("posts", "codebase_community")
    df2 = df2[df2["Title"] == "Understanding what Dassault iSight is doing?"]
    res = df1.merge(df2, suffixes=["", "_y"], left_on="Id", right_on='OwnerUserId')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res.to_csv("results/sql_result_578.csv",index=False)


def q_537():
    df1 = _get_df("users", "codebase_community")
    df1 = df1[df1["DisplayName"] == "csgillespie"]

    df2 = _get_df("posts", "codebase_community")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="Id", right_on='OwnerUserId')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res = res["Id"].mean()
    res = pd.Series(res)
    res.to_csv("results/sql_result_537.csv",index=False)


def q_671():
    df1 = _get_df("badges", "codebase_community")
    df1 = df1[df1["Name"] == "Autobiographer"].nsmallest(n=1, columns=["Date"])

    df2 = _get_df("users", "codebase_community")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="UserId", right_on='Id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res.to_csv("results/sql_result_671.csv",index=False)


def q_682():
    df1 = _get_df("posts", "codebase_community")
    df1 = df1[(df1["CreaionDate"] >= "2010-01-01 0:0:0.0") & (df1["CreaionDate"] <= "2011-01-01 0:0:0.0")].nlargest(n=1,
                                                                                                                    columns=[
                                                                                                                        "FavoriteCount"])

    df2 = _get_df("users", "codebase_community")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="OwnerUserId", right_on='Id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res.to_csv("results/sql_result_682.csv",index=False)


def q_1472():
    df1 = _get_df("customers", "debit_card_specializing")
    df1 = df1[df1["Segment"] == "LAM"]

    df2 = _get_df("yearmonth", "debit_card_specializing")
    df2 = df2[(df2["Date"] >= 201201) & (df2["Date"] <= 201212)]
    res = df1.merge(df2, suffixes=["", "_y"], left_on="CustomerID", right_on='CustomerID')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)
    res = res.nsmallest(n=1, columns=["Consumption"])
    res.to_csv("results/sql_result_1472.csv",index=False)


def q_1514():
    df1 = _get_df("transactions_1k", "debit_card_specializing")
    df1 = df1[df1["Date"] == "2012-08-24"]
    df1 = df1[df1["Time"] == "16:25:00"]

    df2 = _get_df("customers", "debit_card_specializing")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="CustomerID", right_on='CustomerID')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res.to_csv("results/sql_result_1514.csv",index=False)


def q_1505():
    df1 = _get_df("customers", "debit_card_specializing")
    df1 = df1[df1["Currency"] == "EUR"]

    df2 = _get_df("yearmonth", "debit_card_specializing")
    df2 = df2[df2["Consumption"] >= 1000]
    res = df1.merge(df2, suffixes=["", "_y"], left_on="CustomerID", right_on='CustomerID')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res = res["CustomerID"].count()
    res = pd.Series(res)
    res.to_csv("results/sql_result_1505.csv",index=False)


def q_1473():
    df1 = _get_df("customers", "debit_card_specializing")
    df1 = df1[df1["Segment"] == "SME"]

    df2 = _get_df("yearmonth", "debit_card_specializing")
    df2 = df2[(df2["Date"] >= 201301) & (df2["Date"] <= 201312)]
    res = df1.merge(df2, suffixes=["", "_y"], left_on="CustomerID", right_on='CustomerID')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res = res["CustomerID"].mean()
    res = pd.Series(res)
    res.to_csv("results/sql_result_1473.csv", index=False)


def q_137():
    df1 = _get_df("account", "financial")
    df2 = _get_df("loan", "financial")
    df2 = df2[(df2["status"] == "C") | (df2["status"] == "D")]
    res = df1.merge(df2, suffixes=["", "_y"], left_on="account_id", right_on='account_id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    df3 = _get_df("district", "financial")
    df3 = df3[df3["district_id"] == 1]
    res = res.merge(df3, suffixes=["", "_y"], left_on="district_id", right_on='district_id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res.to_csv("results/sql_result_137.csv", index=False)


def q_112():
    df1 = _get_df("client", "financial")
    df1 = df1[df1["gender"] == "F"]
    df1 = df1[df1["birth_date"] == "1976-01-29"]

    df2 = _get_df("district", "financial")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="district_id", right_on='district_id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res.to_csv("results/sql_result_112.csv", index=False)


def q_92():
    df1 = _get_df("client", "financial")
    df1 = df1[df1["gender"] == "F"]

    df2 = _get_df("district", "financial")
    df2 = df2[(df2["A11"] >= 6000) & (df2["A11"] <= 10000)]
    res = df1.merge(df2, suffixes=["", "_y"], left_on="district_id", right_on='district_id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)
    res = res["district_id"].nunique()
    res = pd.Series(res)
    res.to_csv("results/sql_result_92.csv", index=False)


def q_1179():
    df1 = _get_df("Patient", "thrombosis_prediction")
    df1 = df1[df1["Diagnosis"] == "SLE"]
    df1 = df1[df1["Description"] == "1994-02-19"]

    df2 = _get_df("Examination", "thrombosis_prediction")
    df2 = df2[df2["Examination Date"] == "1993-11-12"]
    res = df1.merge(df2, suffixes=["", "_y"], left_on="ID", right_on='ID')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res.to_csv("results/sql_result_1179.csv", index=False)


def q_1155():
    df1 = _get_df("Patient", "thrombosis_prediction")

    df2 = _get_df("Laboratory", "thrombosis_prediction")
    df2 = df2[df2["LDH"] >= 500]
    res = df1.merge(df2, suffixes=["", "_y"], left_on="ID", right_on='ID')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res.to_csv("results/sql_result_1155.csv", index=False)


def q_1251():
    df1 = _get_df("Patient", "thrombosis_prediction")

    df2 = _get_df("Laboratory", "thrombosis_prediction")
    df2 = df2[df2["IGG"] >= 2000]
    res = df1.merge(df2, suffixes=["", "_y"], left_on="ID", right_on='ID')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res = res["ID"].nunique()
    res = pd.Series(res)
    res.to_csv("results/sql_result_1251.csv", index=False)


def q_327():
    df1 = _get_df("molecule", "toxicology")
    df1 = df1[df1["label"] == "-"]
    df2 = _get_df("atom", "toxicology")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="molecule_id", right_on='molecule_id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    res = res.groupby(by=["molecule_id"])["atom_id"].agg('count').rename("count").to_frame().reset_index()
    res = res[res["count"] >= 5]
    res.to_csv("results/sql_result_327.csv", index=False)


def q_243():
    df1 = _get_df("atom", "toxicology")
    df1 = df1[(df1["element"] == "p") | (df1["element"] == "n")]

    df2 = _get_df("connected", "toxicology")
    res = df1.merge(df2, suffixes=["", "_y"], left_on="atom_id", right_on='atom_id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)

    df3 = _get_df("bond", "toxicology")
    res = res.merge(df3, suffixes=["", "_y"], left_on="bond_id", right_on='bond_id')
    res.drop(res.filter(regex='_y$').columns, axis=1, inplace=True)
    res.to_csv("results/sql_result_243.csv", index=False)


def q_195():
    df1 = _get_df("bond", "toxicology")
    res = df1.groupby(by=["bond_type"])["bond_id"].agg('count').rename("count").to_frame().reset_index().nlargest(n=1,
                                                                                                                  columns=[
                                                                                                                      "count"])
    res.to_csv("results/sql_result_195.csv", index=False)


res_dict = {"468":q_468(),"340":q_340(),"341":q_341(),"806":q_806(),"737":q_737(),"751":q_751(),
            "872":q_872(),"875":q_875(),"933":q_933(),"892":q_892(),"1096":q_1096(),"1145":q_1145(),
            "1147":q_1147(),"1032":q_1032(),"1381":q_1381(),"1344":q_1344(),"1405":q_1405(),"1334":q_1334(),
            "17":q_17(),"39":q_39(),"578":q_578(),"537":q_537(),"671":q_671(),"682":q_682(),"1472":q_1472(),
            "1514":q_1514(),"1505":q_1505(),"1473":q_1473(),"137": q_137(),"112":q_112(),"92":q_92(),
            "1179":q_1179(),"1155":q_1155(),"1251":q_1251(),"327":q_327(),"243":q_243(),"195":q_195()}

if __name__ == "__main__":

    for k in res_dict.keys():
        res_dict[k]

