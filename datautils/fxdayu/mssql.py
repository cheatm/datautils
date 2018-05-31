import pymssql
from datautils.sql import make_command
from datautils.fxdayu.basic import SingleReader, MultiReader, single_fields_mapper
import pandas as pd


# server = "192.168.0.102"
# user = "SA"
# password = "Xinger520"
#
# table = "dbo.ASHAREBALANCESHEET"
#
# conn = pymssql.connect(server, user, password, "dbo")
#


FIELDS_MAP = {}
ANTI_FIELDS_MAP = {}


def reverse_map(dct):
    return {value: key for key, value in dct.items()}


class SQLSingleReader(SingleReader):

    def __init__(self, conn, table):
        self.conn = conn
        self.table = table

    def __call__(self, index=None, fields=None, **filters):
        return self.read(index=None, fields=fields, **filters)

    def read(self, index=None, fields=None, **filters):
        command = make_command(self.table, fields, **filters)
        print(command)
        return pd.read_sql(make_command(self.table, fields, **filters), self.conn)


def load_conf(dct):
    methods = {}
    cp = dct["connection_params"]
    conn = pymssql.connect(*cp)
    db_map = dct["db_map"]
    for method, table in db_map.items():
        methods[method.lower()] = SQLSingleReader(conn, table)
    for key, mapper in dct.get("fields_map", {}).items():
        reader = methods[key.lower()]
        reader.read = single_fields_mapper(mapper, reverse_map(mapper))(reader.read)
    return methods


if __name__ == '__main__':
    import json

    config = json.load(open(r'D:\mongoutil\confs\mssql-conf-template.json'))
    methods = load_conf(config)
    print(methods)