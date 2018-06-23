from datautils.fxdayu.basic import SingleMapReader
import pandas as pd
from datautils.sql import make_command
import sqlite3
import numpy as np
import logging
import os


class LocalSqlite(SingleMapReader):

    def __init__(self, sqlite_file, table, view="", mapper=None):
        super(LocalSqlite, self).__init__(mapper)
        assert os.path.isfile(sqlite_file)
        self.file = sqlite_file
        self.table = table
        self.view = view
        self.columns = self.get_columns()
        self._predefine = set()
        self._predefine.update(self.mapper.keys())
        self._predefine.update(self.mapper.values())

        if self.view:
            self._predefine.update(
                ["{}.{}".format(self.view, column) for column in self.columns]
            )
    
    def predefine(self):
        return self._predefine
        
    def get_columns(self):
        conn = sqlite3.Connection(self.file)
        cursor = conn.cursor()
        cursor.execute("select * from %s" % self.table)
        return set([record[0] for record in cursor.description])

    def read(self, fields=None, **filters):
        if fields is not None:
            if isinstance(fields, str):
                fields = {fields}
            fields = self.columns.intersection(set(fields))
        command = make_command(self.table, fields, **filters)
        conn = sqlite3.Connection(self.file)
        r = pd.read_sql(command, conn)
        conn.close()
        return r


def load_conf(dct):
    from datautils.tools.field_mapper import read
    if 'map_file' in dct:
            fields_map = read(dct["map_file"])
    else:
        fields_map = {}
        
    sqlite_file = dct["file"]
    methods = {}
    
    for key, table in dct.get("table_map", {}).items():
        mapper = fields_map.get(key, {})
        reader = LocalSqlite(sqlite_file, table, key, mapper)
        methods[key] = reader
        methods.setdefault("predefine", {})[key] = reader.predefine
    return methods


def main():
    dct = {
        "type": "sqlite",
        "file": "D:/sqlite_data/ZYYXData2.sqlite",
        "map_file": "C:/Users/bigfish01/Documents/Python Scripts/datautils/name_map.xlsx",
        "table_map": {
            "zyyx.conForecastStk": "CON_FORECAST_STK"
        }
    }
    methods = load_conf(dct)
    reader = methods["zyyx.conForecastStk"]
    print(reader(symbol="000001.SZ", trade_date=(20180101, None)))
