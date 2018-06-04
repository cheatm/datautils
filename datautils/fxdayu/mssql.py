import pymssql
from datautils.sql import make_command
from datautils.fxdayu.basic import SingleReader, MultiReader, single_fields_mapper, DailyReader
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


from collections import Iterable


class SQLSingleReader(SingleReader):

    def __init__(self, conn, table):
        self.conn = conn
        self.table = table
        self.limits = self._find_limits()
    
    def _find_limits(self):
        schema, table_name = self.table.split(".", 1)
        cmd = make_command(
            "information_schema.columns", ["COLUMN_NAME"], 
            TABLE_NAME=table_name,
            TABLE_SCHEMA=schema
        )
        return set(pd.read_sql(cmd, self.conn)["COLUMN_NAME"])

    def select_fields(self, fields):
        if fields is None:
            fields = self.limits
        elif isinstance(fields, Iterable) and not isinstance(fields, str):
            fields = set(fields)
        else:
            fields = {fields}
        return self.limits.intersection(fields)
    
    def select_filters(self, filters):
        fields = self.limits.intersection(set(filters))
        return {name: filters[name] for name in fields}

    def __call__(self, index=None, fields=None, **filters):
        return self.read(index=None, fields=fields, **filters)

    def read(self, index=None, fields=None, **filters):
        fields = self.select_fields(fields)
        filters = self.select_filters(filters)
        command = make_command(self.table, fields, **filters)
        return pd.read_sql(make_command(self.table, fields, **filters), self.conn)


class TradeDateReader(SQLSingleReader):

    def read(self, index=None, fields=None, **filters):
        return super(TradeDateReader, self).read(index, fields, **filters).drop_duplicates()


class SQLDailyReader(DailyReader):

    def __init__(self, conn, table):
        self.reader = SQLSingleReader(conn, table)

    def __call__(self, symbols, start, end, fields=None):
        return self.read(None, fields, trade_date=(start, end), symbol=symbols)

    def read(self, index=None, fields=None, **filters):
        return self.reader.read(index, fields, **filters)


class IndexConsReader(SQLSingleReader):

     def __call__(self, index=None, fields=None, **filters):
        d1 = self.read(index=None, fields=fields, **filters)
        if "out_date" in filters:
            filters["out_date"] = None
        else:
            return d1
        d2 = self.read(index=None, fields=fields, **filters)
        return pd.concat([d1, d2], ignore_index=True)
    

class SecSuspReader(SQLSingleReader):

     def __call__(self, index=None, fields=None, **filters):
        d1 = self.read(index=None, fields=fields, **filters)
        if "resu_date" in filters:
            filters["resu_date"] = None
        else:
            return d1
        d2 = self.read(index=None, fields=fields, **filters)
        return pd.concat([d1, d2], ignore_index=True)
    


SPECIAL_CLS = {
    "trade_cal": TradeDateReader,
    "daily": SQLDailyReader,
    "index_cons": IndexConsReader,
    "sec_susp": SecSuspReader
}


def load_conf(dct):
    methods = {}
    cp = dct["connection_params"]
    conn = pymssql.connect(*cp)
    db_map = dct["db_map"]
    for method, table in db_map.items():
        if len(table) == 0:
            continue
        name = method.lower()
        cls = SPECIAL_CLS.get(name, SQLSingleReader)
        methods[name] = cls(conn, table)
    for key, mapper in dct.get("fields_map", {}).items():
        reader = methods[key.lower()]
        reader.read = single_fields_mapper(mapper, reverse_map(mapper))(reader.read)
    return methods


if __name__ == '__main__':
    conn = pymssql.connect("172.16.100.7", "bigfish01", "bigfish01@0514", "NWindDB")
    reader = SQLSingleReader(conn, "dbo.ASHAREDESCRIPTION")

    # print(reader._find_limits())