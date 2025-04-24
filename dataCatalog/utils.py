# TODO save class to yml
import pandas as pd
import json
import yaml
from uuid import uuid4

base_products = ['sum', 'avg', 'absolute', 'relative']


def product_to_config(dataset_name, info, row_wise: bool, column_wise: bool):
    if _config_exists(dataset_name):
        print('Config file already exists and wont be created ')
        return
    if not _products_exists(dataset_name):
        print('Could not find the data product please make sure it is at the correct location')
        return

    location = "../data/data_products/EDGAR_2024_GHG/" + dataset_name + ".csv"
    df = pd.read_csv(location,index_col=0)

    yml_dict = {'name': dataset_name, 'id': uuid4(), 'location': location, 'description': get_description(dataset_name)}

    if row_wise:
        yml_dict['row_names'] = df.index.to_list()
    else:
        yml_dict['row_names'] =[]

    if column_wise:
        yml_dict['column_names'] = df.columns.to_list()
    else:
        yml_dict['column_names'] =[]

    if 'name_readable' in info:
        yml_dict['name_readable'] = info['name_readable']
    else:
        yml_dict['name_readable'] =[]

    products, product_names = addProducts(info)
    products, product_names = addBaseProducts(products, product_names,dataset_name)

    yml_dict['products'] = products
    yml_dict['product_names'] = product_names

    with open('configs/'+dataset_name+'.yml', 'w') as yaml_file:
        yaml.dump(yml_dict, yaml_file, default_flow_style=False)


def addBaseProducts(products, product_names, dataset_name):
    for prod in base_products:
        url = "http://127.0.0.1:5000/EDGAR_2024_GHG/" + dataset_name + "/" + prod
        products.append({'name': prod, 'description': prod, 'base_api': url})
        product_names.append(prod)

    return products, product_names


def addProducts(info):
    products = []
    product_names = []
    for product in info['products']:
        prod_dict ={}
        product_names.append(product['name'])

        if 'name' not in product:
            print('you provided unnamed data products, all products need to be named data set wiil not be created')
            return

        prod_dict['name'] = product['name']

        if 'base_api' not in product:
            print('you provided a data product without an api, all products need an url to call data set wiil not be '
                  'created')
            return

        prod_dict['base_api'] = product['base_api']

        try:
            prod_dict['description'] = product['description']
        except:
            prod_dict['description'] = ''
        products.append(prod_dict)

    return products, product_names


def _config_exists(dataset_name):
    try:
        with open("./configs/" + dataset_name + ".yml") as stream:
            data = yaml.safe_load(stream)
            return True
    except FileNotFoundError:
        return False


def _products_exists(dataset_name):
    try:
        df = pd.read_csv("../data/data_products/EDGAR_2024_GHG/" + dataset_name + ".csv")
        return True
    except FileNotFoundError:
        return False


def get_description(dataset_name):
    try:
        return json.load(open("../data/EDGAR_2024_GHG/metadata_automatic.json"))[dataset_name][
            "description"]
    except:
        print(
            'Could not a description for the dataset and will proceed without it, this can greatly impact performance')
        return ""


if __name__ == "__main__":
    file = 'GHG_per_GDP_by_country'

    test = {'products': [{'name': 'test', 'description': 'test', 'base_api': 'test'}]}
    product_to_config('GHG_per_GDP_by_country',test, False, True)

    df = pd.read_csv("../data/data_products/EDGAR_2024_GHG/" + file + ".csv")
    print(df.columns.to_list())
