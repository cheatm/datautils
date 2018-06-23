import pymssql
from datautils.sql import make_command
from datautils.fxdayu.basic import SingleReader, MultiReader, single_fields_mapper, DailyReader, SingleMapReader
import pandas as pd
import numpy as np


FIELDS_MAP = {}
ANTI_FIELDS_MAP = {}


def reverse_map(dct):
    return {value: key for key, value in dct.items()}


from collections import Iterable


class SQLSingleReader(SingleMapReader):

    def __init__(self, conn, table, mapper=None):
        super(SQLSingleReader, self).__init__(mapper)
        self.conn = conn
        self.table = table
        self._limits = set()
        self.limits()
        for limit in self._limits:
            self.mapper["%s.%s" % (self.table, limit)] = limit
        
    def predefine(self):
        limits = set(self.mapper.keys())
        limits.update(set(self.mapper.values()))
        return limits
    
    def limits(self):
        if self._limits:
            return self._limits
        else:
            self._limits = self._find_limits()
            return self._limits
    
    def _find_limits(self):
        schema, table_name = self.table.split(".", 1)
        cmd = make_command(
            "information_schema.columns", ["COLUMN_NAME"], 
            TABLE_NAME=table_name,
            TABLE_SCHEMA=schema
        )
        data = set(pd.read_sql(cmd, self.conn.connection())["COLUMN_NAME"])
        return data

    def select_fields(self, fields):
        if fields is None:
            return None
        elif isinstance(fields, Iterable) and not isinstance(fields, str):
            fields = set(fields)
        else:
            fields = {fields}
        return self.limits().intersection(fields)
    
    def select_filters(self, filters):
        fields = self.limits().intersection(set(filters))
        return {name: filters[name] for name in fields}

    def read(self, fields=None, **filters):
        fields = self.select_fields(fields)
        filters = self.select_filters(filters)
        command = make_command(self.table, fields, **filters)
        with self.conn as conn:
            data = pd.read_sql(command, conn).select_dtypes(include=[np.object, np.number]).fillna(0)
            
        return data


class TradeDateReader(SQLSingleReader):

    def read(self, fields=None, **filters):
        return super(TradeDateReader, self).read(fields, **filters).drop_duplicates()


class SQLDailyReader(DailyReader):

    def __init__(self, reader):
        self.reader = reader

    def __call__(self, symbols, start, end, fields=None):
        return self.read(fields, trade_date=(start, end), symbol=symbols)

    def read(self, fields=None, **filters):
        return self.reader(None, fields, **filters)


class IndexConsReader(SQLSingleReader):

     def __call__(self, index=None, fields=None, **filters):
        d1 = super(IndexConsReader, self).__call__(index=None, fields=fields, **filters)
        if "out_date" in filters:
            filters["out_date"] = None
        else:
            return d1
        d2 = super(IndexConsReader, self).__call__(index=None, fields=fields, **filters)
        return pd.concat([d1, d2], ignore_index=True)
    

class SecSuspReader(SQLSingleReader):

     def __call__(self, index=None, fields=None, **filters):
        d1 = super(SecSuspReader, self).__call__(index=None, fields=fields, **filters)
        if "resu_date" in filters:
            filters["resu_date"] = None
        else:
            return d1
        d2 = super(SecSuspReader, self).__call__(index=None, fields=fields, **filters)
        return pd.concat([d1, d2], ignore_index=True)
    

def create_external(conn, mapper):
    external = {}

    tables = pd.read_sql("select TABLE_SCHEMA,TABLE_NAME from information_schema.TABLES", conn.connection())
    for table in (tables.TABLE_SCHEMA + "." + tables.TABLE_NAME):
        external[table] = SQLSingleReader(conn, table, mapper.get(table, {}))
    return external
    # return {table: SQLSingleReader(conn, table) for table in (tables.TABLE_SCHEMA + "." + tables.TABLE_NAME)}
        

SPECIAL_CLS = {
    "jz.secTradeCal": TradeDateReader,
    "daily": SQLDailyReader,
    "lb.indexCons": IndexConsReader,
    "lb.secSusp": SecSuspReader
}


class MSSQKConControler(object):
    
    def __init__(self, *args , **kwargs):
        self.args = args
        self.kwargs = kwargs
        self._con = None
    
    def __enter__(self):
        self._con = pymssql.connect(*self.args, **self.kwargs)
        return self._con

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._con.close()
        self._con = None
    
    def connection(self):
        if self._con is not None:
            return self._con
        else:
            self._con = pymssql.connect(*self.args, **self.kwargs)
            return self._con

    def close(self):
        if self._con is not None:
            self._con.close
        self._con = None


def load_conf(dct):
    from datautils.tools.field_mapper import read
    from datautils.fxdayu.basic import view_map

    methods = {}
    cp = dct["connection_params"]
    conn = MSSQKConControler(*cp)
    db_map = dct["db_map"]
    if 'map_file' in dct:
        fields_map = read(dct["map_file"])
    else:
        fields_map = {}
    for method, table in db_map.items():
        if len(table) == 0:
            continue
        cls = SPECIAL_CLS.get(method, SQLSingleReader)
        mapper = fields_map.get(table, {})
        if issubclass(cls, SingleMapReader):
            methods[method] = cls(conn, table, mapper)
        else:
            methods[method] = cls(SQLSingleReader(conn, table, mapper))
    if dct.get("external", False):
        # methods["external"] = create_external(conn, fields_map)
        methods.update(create_external(conn, fields_map))
    
    if dct.get("predefine", False):
        predefine = {}
        for key, method in methods.items():
            view = view_map.get(key, key)
            if not isinstance(method, SQLSingleReader):
                if hasattr(method, "reader"):
                    method = method.reader
                else:
                    continue
            predefine[view] = method.predefine
        
        # for key, method in methods.get("external", {}).items():
        #     predefine[key] = method.predefine
        for name in dct.get("exclude", []):
            predefine.pop(name, None)
        methods["predefine"] = predefine
    conn.close()
    return methods


def main():
    import json
    from datautils.fxdayu.basic import DataAPI
    conf = json.load(open(r"C:\Users\bigfish01\Documents\Python Scripts\datautils\confs\mixed-conf.json"))
    api = DataAPI(conf[1])
    


if __name__ == '__main__':
    # main()
    cc = MSSQKConControler()
    with cc as c:
        print(c)