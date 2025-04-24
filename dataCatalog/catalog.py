from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List

#TODO create script that generates yml
#TODO create script that saves changes to
#TODO proper logging
#TODO connect to graph db with id etc for changes

@dataclass
class Dataset(ABC):
    id: int
    name: int
    name_readable: str
    describtion: str
    location: str
    products: list[dataProduct]
    columns: list[Column]
    rows: list[Row]

    @abstractmethod
    def __init__(self):
        ''' To override '''
        pass


    @abstractmethod
    def getNeo4jrepresentation(self):
        ''' To override '''
        pass

    def addProduct(self,product):
        self.products.append(product)

    def getProducts(self):
        return self.products

    def getColumns(self):
        return self.columns

    def getRows(self):
        return self.rows

    def getProduct(self):
        pass

    def deleteProduct(self,product):
        pass

@dataclass
class dataProduct(ABC):
    # generall info
    id: int
    dataset_id: int
    describtion: str

    # important for processing
    columns: [Column]
    rows: [Row]

    # API stuff here
    base_api: str

    @abstractmethod
    def __init__(self):
        ''' To override '''
        pass

    #@abstractmethod
    #def getNeo4jrepresentation(self):
    #    ''' To override '''
    #    pass

    def getColumns(self):
        return self.columns

    def getRows(self):
        return self.rows

@dataclass
class Row(ABC):
    name: str
    id: int

    def __init__(self, name, row_id):
        self.id = row_id
        self.name = name

    def getNeo4jrepresentation(self):
        pass

@dataclass
class Column(ABC):
    name: str
    id: int

    def __init__(self, name, col_id):
        self.id = col_id
        self.name = name

    def getNeo4jrepresentation(self):

        return  f"(c_{self.id}:Column&Catalog {{name:'{self.name}', id:'{self.id}'}}),\n"

if __name__ == "__main__":
    print('test')