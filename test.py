# INITIALIZATION
from __future__ import annotations
#import pycob as cob
import pandas as pd
import sqlite3 as db
from dataclasses import dataclass, asdict
from typing import List
from datetime import datetime
import json


# DATA MODEL
# Python @dataclass is a new feature in Python 3.7 that allows you to define a class with a bunch of attributes and their types.
# It just makes it easier to define a class with a bunch of attributes without having to define the __init__ method.
@dataclass
class Datasets:
    datasets: list[Dataset]

    def to_dataframe(self) -> pd.DataFrame:
        records = []

        for dataset in self.datasets:
            records.extend(dataset.to_dataframe().to_dict('records'))

        return pd.DataFrame(records)

    def get_dataset_index(self, dataset_name: str) -> int:
        for i in range(len(self.datasets)):
            if self.datasets[i].name == dataset_name:
                return i

        return -1


@dataclass
class Dataset:
    name: str
    readable_name: str
    description: str
    tables: list[Table]

    def to_dataframe(self) -> pd.DataFrame:
        records = []

        for table in self.tables:
            row = {}
            row['dataset_name'] = self.name
            row['dataset_readable_name'] = self.readable_name
            row['dataset_description'] = self.description
            row['table_name'] = table.name
            row['table_readable_name'] = table.readable_name
            row['table_description'] = table.description
            row['row_count'] = table.row_count
            row['table_last_updated'] = table.last_updated.strftime("%Y-%m-%d %H:%M")
            row['table_type'] = table.type

            records.append(row)

        return pd.DataFrame(records)

    def get_table_index(self, table_name: str) -> int:
        for i in range(len(self.tables)):
            if self.tables[i].name == table_name:
                return i

        return -1


@dataclass
class Table:
    name: str
    readable_name: str
    description: str
    last_updated: datetime
    type: str
    columns: list[Column]
    sample: pd.DataFrame
    row_count: int

    def to_dataframe(self) -> pd.DataFrame:
        records = []

        for column in self.columns:
            row = {}
            row['column_name'] = column.name
            row['column_readable_name'] = column.readable_name
            row['column_description'] = column.description
            row['column_data_type'] = column.data_type
            row['column_nullable'] = column.nullable
            row['column_min'] = column.min
            row['column_max'] = column.max
            row['column_mean'] = column.mean
            row['column_num_distinct'] = column.num_distinct
            row['column_num_null'] = column.num_null
            row['column_num_rows'] = column.num_rows

            records.append(row)

        return pd.DataFrame(records)


@dataclass
class Column:
    name: str
    readable_name: str
    description: str
    data_type: str
    nullable: bool
    min: float
    max: float
    mean: float
    num_distinct: int
    num_null: int
    num_rows: int

