import json
import os

import pandas as pd
import yaml
from flask import Flask, request

app = Flask(__name__)
port = int(os.environ.get('PORT', 5000))


@app.route('/catalog', methods=['GET'])
def getCatalog():
    content = json.loads(request.data)
    file = content['file']
    if "/" in file:
        file = file.split("/")[-1]

    try:
        with open("../data/Catalogs/catalog.yml") as stream:
            catalog = yaml.safe_load(stream)
    except FileNotFoundError:
        return "could not find the main catalog"
    for collection in catalog:
        collection = catalog[collection]
        if file in collection['products']:
            try:
                with open("../data/Catalogs/" + collection['name'] + ".yml") as stream:
                    collection_dict = yaml.safe_load(stream)
                    return collection_dict[file]
            except FileNotFoundError:
                return "could not find the specific collection catalog"
            except KeyError:
                return collection_dict
    return {"text":"No fitting collection found"}

@app.route('/catalog/collection', methods=['GET'])
def getCatalogCollection():
    content = json.loads(request.data)
    file = content['file']
    if "/" in file:
        file = file.split("/")[-1]
    try:
        with open("../data/Catalogs/catalog.yml") as stream:
            catalog = yaml.safe_load(stream)
    except FileNotFoundError:
        return "could not find the main catalog"
    for collection in catalog:
        if file == collection:
            collection = catalog[collection]
            try:
                with open("../data/Catalogs/" + collection['name'] + ".yml") as stream:
                    collection_dict = yaml.safe_load(stream)
                    return collection_dict

            except FileNotFoundError:
                return "could not find the specific collection catalog"
    return {"text": "No fitting collection found"}

@app.route('/catalog/columns', methods=['GET'])
def getCatalogColumns():
    content = json.loads(request.data)
    file = content['file']

    if "/" in file:
        file = file.split("/")[-1]

    try:
        with open("../data/Catalogs/catalog.yml") as stream:
            catalog = yaml.safe_load(stream)
    except FileNotFoundError:
        return "could not find the main catalog"

    for collection in catalog:
        if file in catalog[collection]['products']:
            try:
                with open("../data/Catalogs/" + collection + ".yml") as stream:
                    collection_dict = yaml.safe_load(stream)
                    return collection_dict[file]['columns']
            except FileNotFoundError:
                return "could not find the specific collection catalog"
            except KeyError:
                return collection_dict
    return {"text": "No fitting catalog columns found"}

# ----------------------california_schools-----------------------
@app.route('/products/california_schools/frpm', methods=['GET'])
def get_frpm():
    # read from data catalog
    df = pd.read_csv('../data/california_schools/frpm.csv')
    return {'data': df.to_json()}


@app.route('/products/california_schools/schools', methods=['GET'])
def get_school():
    # read from data catalog
    df = pd.read_csv('../data/california_schools/schools.csv',dtype={"CharterNum": str})
    return {'data': df.to_json()}


@app.route('/products/california_schools/satscores', methods=['GET'])
def get_satscores():
    # read from data catalog
    df = pd.read_csv('../data/california_schools/satscores.csv')
    return {'data': df.to_json()}


# ----------------------card_games-----------------------
@app.route('/products/card_games/cards', methods=['GET'])
def get_cards():
    # read from data catalog
    df = pd.read_csv('../data/card_games/cards.csv',dtype={"duelDeck": str, "flavorName": str, "frameVersion": str, "loyalty": str,
                                "originalReleaseDate": str})
    return {'data': df.to_json()}


@app.route('/products/card_games/sets', methods=['GET'])
def get_sets():
    # read from data catalog
    df = pd.read_csv('../data/card_games/sets.csv')
    return {'data': df.to_json()}


@app.route('/products/card_games/set_translations', methods=['GET'])
def get_set_translations():
    # read from data catalog
    df = pd.read_csv('../data/card_games/set_translations.csv')
    return {'data': df.to_json()}


@app.route('/products/card_games/rulings', methods=['GET'])
def get_rulings():
    # read from data catalog
    df = pd.read_csv('../data/card_games/rulings.csv')
    return {'data': df.to_json()}


@app.route('/products/card_games/legalities', methods=['GET'])
def get_legalities():
    # read from data catalog
    df = pd.read_csv('../data/card_games/legalities.csv')
    return {'data': df.to_json()}


@app.route('/products/card_games/foreign_data', methods=['GET'])
def get_foreign_data():
    # read from data catalog
    df = pd.read_csv('../data/card_games/foreign_data.csv')
    return {'data': df.to_json()}


# ----------------------codebase_community-----------------------
@app.route('/products/codebase_community/badges', methods=['GET'])
def get_badges():
    # read from data catalog
    df = pd.read_csv('../data/codebase_community/badges.csv')
    return {'data': df.to_json()}


@app.route('/products/codebase_community/comments', methods=['GET'])
def get_comments():
    # read from data catalog
    df = pd.read_csv('../data/codebase_community/comments.csv')
    return {'data': df.to_json()}


@app.route('/products/codebase_community/votes', methods=['GET'])
def get_votes():
    # read from data catalog
    df = pd.read_csv('../data/codebase_community/votes.csv')
    return {'data': df.to_json()}


@app.route('/products/codebase_community/users', methods=['GET'])
def get_users():
    # read from data catalog
    df = pd.read_csv('../data/codebase_community/users.csv')
    return {'data': df.to_json()}


@app.route('/products/codebase_community/tags', methods=['GET'])
def get_tags():
    # read from data catalog
    df = pd.read_csv('../data/codebase_community/tags.csv')
    return {'data': df.to_json()}


@app.route('/products/codebase_community/posts', methods=['GET'])
def get_posts():
    # read from data catalog
    df = pd.read_csv('../data/codebase_community/posts.csv')
    return {'data': df.to_json()}


@app.route('/products/codebase_community/postLinks', methods=['GET'])
def get_postLinks():
    # read from data catalog
    df = pd.read_csv('../data/codebase_community/postLinks.csv')
    return {'data': df.to_json()}


@app.route('/products/codebase_community/postHistory', methods=['GET'])
def get_postHistory():
    # read from data catalog
    df = pd.read_csv('../data/codebase_community/postHistory.csv')
    return {'data': df.to_json()}


# ----------------------debit_card_specializing-----------------------
@app.route('/products/debit_card_specializing/customers', methods=['GET'])
def get_customers():
    # read from data catalog
    df = pd.read_csv('../data/debit_card_specializing/customers.csv')
    return {'data': df.to_json()}


@app.route('/products/debit_card_specializing/gasstations', methods=['GET'])
def get_gasstations():
    # read from data catalog
    df = pd.read_csv('../data/debit_card_specializing/gasstations.csv')
    return {'data': df.to_json()}


@app.route('/products/debit_card_specializing/yearmonth', methods=['GET'])
def get_yearmonth():
    # read from data catalog
    df = pd.read_csv('../data/debit_card_specializing/yearmonth.csv')
    return {'data': df.to_json()}


@app.route('/products/debit_card_specializing/transactions_1k', methods=['GET'])
def get_transactions_1k():
    # read from data catalog
    df = pd.read_csv('../data/debit_card_specializing/transactions_1k.csv')
    return {'data': df.to_json()}


@app.route('/products/debit_card_specializing/products', methods=['GET'])
def get_products():
    # read from data catalog
    df = pd.read_csv('../data/debit_card_specializing/products.csv')
    return {'data': df.to_json()}


# ----------------------european_football_2-----------------------
@app.route('/products/european_football_2/Country', methods=['GET'])
def get_Country():
    # read from data catalog
    df = pd.read_csv('../data/european_football_2/Country.csv')
    return {'data': df.to_json()}


@app.route('/products/european_football_2/League', methods=['GET'])
def get_League():
    # read from data catalog
    df = pd.read_csv('../data/european_football_2/League.csv')
    return {'data': df.to_json()}


@app.route('/products/european_football_2/Team_Attributes', methods=['GET'])
def get_Team_Attributes():
    # read from data catalog
    df = pd.read_csv('../data/european_football_2/Team_Attributes.csv')
    return {'data': df.to_json()}


@app.route('/products/european_football_2/Team', methods=['GET'])
def get_Team():
    # read from data catalog
    df = pd.read_csv('../data/european_football_2/Team.csv')
    return {'data': df.to_json()}


@app.route('/products/european_football_2/Player_Attributes', methods=['GET'])
def get_Player_Attributes():
    # read from data catalog
    df = pd.read_csv('../data/european_football_2/Player_Attributes.csv')
    return {'data': df.to_json()}


@app.route('/products/european_football_2/Player', methods=['GET'])
def get_Player():
    # read from data catalog
    df = pd.read_csv('../data/european_football_2/Player.csv')
    return {'data': df.to_json()}


@app.route('/products/european_football_2/Match', methods=['GET'])
def get_Match():
    # read from data catalog
    df = pd.read_csv('../data/european_football_2/Match.csv')
    return {'data': df.to_json()}


# ----------------------financial-----------------------
@app.route('/products/financial/account', methods=['GET'])
def get_account():
    # read from data catalog
    df = pd.read_csv('../data/financial/account.csv')
    return {'data': df.to_json()}


@app.route('/products/financial/card', methods=['GET'])
def get_card():
    # read from data catalog
    df = pd.read_csv('../data/financial/card.csv')
    return {'data': df.to_json()}


@app.route('/products/financial/client', methods=['GET'])
def get_client():
    # read from data catalog
    df = pd.read_csv('../data/financial/client.csv')
    return {'data': df.to_json()}


@app.route('/products/financial/disp', methods=['GET'])
def get_disp():
    # read from data catalog
    df = pd.read_csv('../data/financial/disp.csv')
    return {'data': df.to_json()}


@app.route('/products/financial/district', methods=['GET'])
def get_district():
    # read from data catalog
    df = pd.read_csv('../data/financial/district.csv')
    return {'data': df.to_json()}


@app.route('/products/financial/loan', methods=['GET'])
def get_loan():
    # read from data catalog
    df = pd.read_csv('../data/financial/loan.csv')
    return {'data': df.to_json()}


@app.route('/products/financial/trans', methods=['GET'])
def get_trans():
    # read from data catalog

    df = pd.read_csv('../data/financial/trans.csv', dtype = {"bank": str})
    return {'data': df.to_json()}


@app.route('/products/financial/order', methods=['GET'])
def get_order():
    # read from data catalog
    df = pd.read_csv('../data/financial/order.csv')
    return {'data': df.to_json()}


# ----------------------formula_1-----------------------


@app.route('/products/formula_1/circuits', methods=['GET'])
def get_circuits():
    # read from data catalog
    df = pd.read_csv('../data/formula_1/circuits.csv')
    return {'data': df.to_json()}


@app.route('/products/formula_1/constructorResults', methods=['GET'])
def get_constructorResults():
    # read from data catalog
    df = pd.read_csv('../data/formula_1/constructorResults.csv')
    return {'data': df.to_json()}


@app.route('/products/formula_1/constructors', methods=['GET'])
def get_constructors():
    # read from data catalog
    df = pd.read_csv('../data/formula_1/constructors.csv')
    return {'data': df.to_json()}


@app.route('/products/formula_1/constructorStandings', methods=['GET'])
def get_constructorStandings():
    # read from data catalog
    df = pd.read_csv('../data/formula_1/constructorStandings.csv')
    return {'data': df.to_json()}


@app.route('/products/formula_1/drivers', methods=['GET'])
def get_drivers():
    # read from data catalog
    df = pd.read_csv('../data/formula_1/drivers.csv')
    return {'data': df.to_json()}


@app.route('/products/formula_1/driverStandings', methods=['GET'])
def get_driverStandings():
    # read from data catalog
    df = pd.read_csv('../data/formula_1/driverStandings.csv')
    return {'data': df.to_json()}


@app.route('/products/formula_1/lapTimes', methods=['GET'])
def get_lapTimes():
    # read from data catalog
    df = pd.read_csv('../data/formula_1/lapTimes.csv')
    return {'data': df.to_json()}


@app.route('/products/formula_1/pitStops', methods=['GET'])
def get_pitStops():
    # read from data catalog
    df = pd.read_csv('../data/formula_1/pitStops.csv')
    return {'data': df.to_json()}


@app.route('/products/formula_1/qualifying', methods=['GET'])
def get_qualifying():
    # read from data catalog
    df = pd.read_csv('../data/formula_1/qualifying.csv')
    return {'data': df.to_json()}


@app.route('/products/formula_1/races', methods=['GET'])
def get_races():
    # read from data catalog
    df = pd.read_csv('../data/formula_1/races.csv')
    return {'data': df.to_json()}


@app.route('/products/formula_1/results', methods=['GET'])
def get_results():
    # read from data catalog
    df = pd.read_csv('../data/formula_1/results.csv')
    return {'data': df.to_json()}


@app.route('/products/formula_1/status', methods=['GET'])
def get_status():
    # read from data catalog
    df = pd.read_csv('../data/formula_1/status.csv')
    return {'data': df.to_json()}


@app.route('/products/formula_1/seasons', methods=['GET'])
def get_seasons():
    # read from data catalog
    df = pd.read_csv('../data/formula_1/seasons.csv')
    return {'data': df.to_json()}


# ----------------------student_club-----------------------
@app.route('/products/student_club/Attendance', methods=['GET'])
def get_Attendance():
    # read from data catalog
    df = pd.read_csv('../data/student_club/Attendance.csv')
    return {'data': df.to_json()}


@app.route('/products/student_club/Budget', methods=['GET'])
def get_Budget():
    # read from data catalog
    df = pd.read_csv('../data/student_club/Budget.csv')
    return {'data': df.to_json()}


@app.route('/products/student_club/Event', methods=['GET'])
def get_Event():
    # read from data catalog
    df = pd.read_csv('../data/student_club/Event.csv')
    return {'data': df.to_json()}


@app.route('/products/student_club/Expense', methods=['GET'])
def get_Expense():
    # read from data catalog
    df = pd.read_csv('../data/student_club/Expense.csv')
    return {'data': df.to_json()}


@app.route('/products/student_club/Income', methods=['GET'])
def get_Income():
    # read from data catalog
    df = pd.read_csv('../data/student_club/Income.csv')
    return {'data': df.to_json()}


@app.route('/products/student_club/Major', methods=['GET'])
def get_Major():
    # read from data catalog
    df = pd.read_csv('../data/student_club/Major.csv')
    return {'data': df.to_json()}


@app.route('/products/student_club/Member', methods=['GET'])
def get_Member():
    # read from data catalog
    df = pd.read_csv('../data/student_club/Member.csv')
    return {'data': df.to_json()}


@app.route('/products/student_club/Zip_Code', methods=['GET'])
def get_Zip_Code():
    # read from data catalog
    df = pd.read_csv('../data/student_club/Zip_Code.csv')
    return {'data': df.to_json()}


# ----------------------superhero-----------------------
@app.route('/products/superhero/alignment', methods=['GET'])
def get_alignment():
    # read from data catalog
    df = pd.read_csv('../data/superhero/alignment.csv')
    return {'data': df.to_json()}


@app.route('/products/superhero/attribute', methods=['GET'])
def get_attribute():
    # read from data catalog
    df = pd.read_csv('../data/superhero/attribute.csv')
    return {'data': df.to_json()}


@app.route('/products/superhero/colour', methods=['GET'])
def get_colour():
    # read from data catalog
    df = pd.read_csv('../data/superhero/colour.csv')
    return {'data': df.to_json()}


@app.route('/products/superhero/gender', methods=['GET'])
def get_gender():
    # read from data catalog
    df = pd.read_csv('../data/superhero/gender.csv')
    return {'data': df.to_json()}


@app.route('/products/superhero/hero_attribute', methods=['GET'])
def get_hero_attribute():
    # read from data catalog
    df = pd.read_csv('../data/superhero/hero_attribute.csv')
    return {'data': df.to_json()}


@app.route('/products/superhero/hero_power', methods=['GET'])
def get_hero_power():
    # read from data catalog
    df = pd.read_csv('../data/superhero/hero_power.csv')
    return {'data': df.to_json()}


@app.route('/products/superhero/publisher', methods=['GET'])
def get_publisher():
    # read from data catalog
    df = pd.read_csv('../data/superhero/publisher.csv')
    return {'data': df.to_json()}


@app.route('/products/superhero/race', methods=['GET'])
def get_race():
    # read from data catalog
    df = pd.read_csv('../data/superhero/race.csv')
    return {'data': df.to_json()}


@app.route('/products/superhero/superhero', methods=['GET'])
def get_superhero():
    # read from data catalog
    df = pd.read_csv('../data/superhero/superhero.csv')
    return {'data': df.to_json()}


@app.route('/products/superhero/superpower', methods=['GET'])
def get_superpower():
    # read from data catalog
    df = pd.read_csv('../data/superhero/superpower.csv')
    return {'data': df.to_json()}


# ----------------------thrombosis_prediction-----------------------
@app.route('/products/thrombosis_prediction/Examination', methods=['GET'])
def get_Examination():
    # read from data catalog
    df = pd.read_csv('../data/thrombosis_prediction/Examination.csv')
    return {'data': df.to_json()}


@app.route('/products/thrombosis_prediction/Laboratory', methods=['GET'])
def get_Laboratory():
    # read from data catalog
    df = pd.read_csv('../data/thrombosis_prediction/Laboratory.csv')
    return {'data': df.to_json()}


@app.route('/products/thrombosis_prediction/Patient', methods=['GET'])
def get_Patient():
    # read from data catalog
    df = pd.read_csv('../data/thrombosis_prediction/Patient.csv')
    return {'data': df.to_json()}


# ----------------------toxicology-----------------------


@app.route('/products/toxicology/atom', methods=['GET'])
def get_atom():
    # read from data catalog
    df = pd.read_csv('../data/toxicology/atom.csv')
    return {'data': df.to_json()}


@app.route('/products/toxicology/bond', methods=['GET'])
def get_bond():
    # read from data catalog
    df = pd.read_csv('../data/toxicology/bond.csv')
    return {'data': df.to_json()}


@app.route('/products/toxicology/connected', methods=['GET'])
def get_connected():
    # read from data catalog
    df = pd.read_csv('../data/toxicology/connected.csv')
    return {'data': df.to_json()}


@app.route('/products/toxicology/molecule', methods=['GET'])
def get_molecule():
    # read from data catalog
    df = pd.read_csv('../data/toxicology/molecule.csv')
    return {'data': df.to_json()}


# ----------------------END-----------------------

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=port)
