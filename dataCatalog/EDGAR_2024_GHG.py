from catalog import Dataset, dataProduct, Column, Row
from uuid import uuid4
import pickle
import numpy as np
import yaml


class EDGAR_GHG_2024(Dataset):
    def __init__(self, name):
        try:
            with open("./configs/" + name + ".yml") as stream:
                data = yaml.safe_load(stream)
        except FileNotFoundError:
            print("could not find a corresponding config file please make sure it exists")

        self.id = data['id']
        self.name = name
        self.name_readable = data['name_readable']
        self.description = data['description']
        self.product_names = data['product_names']
        self.location = data['location']
        self.products = self.createProducts(data['products'])
        self.columns = self.createColumns(data['column_names'])

    def createColumns(self, columns):
        if not columns:
            return []

        cols = []
        vals = zip(columns, range(len(columns)))
        for name, col_id in vals:
            cols.append(Column(name, col_id))
        return cols

    def createProducts(self,data):
        if not self.product_names:
            return []

        products = []
        for prod_dict in data:
            products.append(EDGAR_GNG_2024_products(prod_dict, self.id))
        return products

    def getNeo4jrepresentation(self):
        cypher = f"CREATE (D:Dataset&Catalog {{name:'{self.name}',name_readable:'{self.name_readable}', description:'{self.description}',location:'{self.location}',id:'{self.id}' }}),\n"
        cypher = cypher.replace('"',"")

        for product in self.products:
            cypher += product.getNeo4jrepresentation()
            cypher += f" (D)-[:hasProduct]->(p_{product.name}),"

        for column in self.columns:
            cypher += column.getNeo4jrepresentation()
            cypher += f" (D)-[:hasColumn]->(c_{column.id}),"

        cypher = cypher[:-1]

        return cypher


class EDGAR_GNG_2024_products(dataProduct):

    def __init__(self, product, dataset_id=None):
        if product['id'] is None:
            self.id = uuid4()
        self.dataset_id = dataset_id
        self.name = product['name']
        self.description = product['description']
        self.column_names = product['column_names']
        self.row_names = product['row_names']
        self.base_api = product['base_api']
        self.columns = self.createColumns()
        self.rows = self.createRows()

    def createColumns(self):
        if not self.column_names:
            return []

        cols = []
        vals = zip(self.column_names, range(len(self.column_names)))
        for name, col_id in vals:
            cols.append(Column(name, col_id))
        return cols

    def createRows(self):
        if not self.row_names:
            return []

        rows = []
        vals = zip(self.row_names, range(len(self.row_names)))
        for name, row_id in vals:
            rows.append(Row(name, row_id))
        return rows

    def getNeo4jrepresentation(self):
        cypher = f"(p_{self.name}:DataProduct&Catalog {{name:'{self.name}', description:'{self.description}',base_api:'{self.base_api}'}}),\n"
        cypher = cypher.replace('"', "")

        return cypher


if __name__ == "__main__":
    with open("configs/archive/EDGAR_GHG_2024_per_capita_by_country.yml") as stream:
        data = yaml.safe_load(stream)
    file = 'EDGAR_GHG_2024_per_capita_by_country'
    testset = EDGAR_GHG_2024(file)
    print(testset.getNeo4jrepresentation())
