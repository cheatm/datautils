import pandas as pd
import tables
from collections import Iterable
import os
from datautils.fxdayu.basic import SingleReader
from datautils.sql import make_command
import sqlite3




class HDFStructure(object):

    def __init__(self, hdf_file, value_name, index_name="/date_flag", column_name="/symbol_flag"):
        if isinstance(hdf_file, tables.File):
            self.file = hdf_file
            self.file_name = self.file.filename
        elif isinstance(hdf_file, str):
            self.file_name = hdf_file
            self.file = tables.File(self.file_name, "r")
        
        self.index_name = index_name
        self.index = self._read_index(index_name).map(str)
        self.column_name = column_name
        self.column = self._read_index(column_name).map(lambda b: b.decode())
        self.value_name = value_name

    def __del__(self):
        if self.file.isopen:
            self.file.close()

    @property
    def value(self):
        if self.file.isopen:
            return self.file.get_node(self.value_name)
        else:
            self.__init__(self.file_name, self.value_name, self.index_name, self.column_name)
            return self.value
    
    def read(self, index, columns):
        index_loc, idx = self._loc(self.index, index)
        column_loc, col = self._loc(self.column, columns)
        data = self.value[index_loc, column_loc]
        return pd.DataFrame(data, idx, col)

    def _read_index(self, name):
        return pd.Index(self.file.get_node(name)[:, 0])

    @staticmethod
    def _loc(index, locs):
        if isinstance(locs, slice):
            slicer = index.slice_indexer(locs.start, locs.stop, locs.step, "loc")
            return slicer, index[slicer]
        elif isinstance(locs, Iterable) and not isinstance(locs, (str, bytes)):
            indexer = list(HDFStructure._iter_loc(index, locs))
            return indexer, index[indexer]
        elif locs is None:
            return slice(None), index[:]
        else:
            indexer = index.get_loc(locs)
            return indexer, index[indexer]

    @staticmethod
    def _iter_loc(index, loc):
        for name in loc:
            try:
                yield index.get_loc(name)
            except:
                pass


class HDFDaily(SingleReader):

    def __init__(self, root):
        self.root = root
        self.files = {}
        for filename in os.listdir(self.root):
            if filename.endswith(".hd5"):
                name = filename[:-4].strip("_")
                v_name = "/" + name
                self.files[name] = HDFStructure(os.path.join(self.root, filename), v_name)
        for name in ["symbol", "trade_date"]:
            s = self.files.pop(name, None)
            if s:
                s.file.close()

    def __call__(self, index=None, fields=None, **filters):
        symbol = filters.get("symbol", None)
        dates = filters.get("trade_date", (None, None))
        dates = slice(*dates)
        if fields is None:
            fields = list(self.files.keys())
        elif isinstance(fields, str):
            fields = [fields]
        
        dct = {}
        for field in fields:
            try:
                struct = self.files[field]
                data = struct.read(dates, symbol)
            except KeyError:
                pass
            except Exception as e:
                print(e)
            else:
                dct[field] = data
        
        pn = pd.Panel.from_dict(dct)
        pn.major_axis.name = "trade_date"
        pn.minor_axis.name = "symbol"
        return pn.to_frame(False).reset_index()


class LocalSqlite(SingleReader):

    def __init__(self, conn, table):
        assert isinstance(conn, sqlite3.Connection)
        self.conn = conn
        self.table = table
    
    def __call__(self, index=None, fields=None, **filters):
        command = make_command(self.table, fields, **filters)
        return pd.read_sql(command, self.conn)


def load_conf(dct):
    r = {}
    root = dct["root_dir"]
    sqlite_path = os.path.join(root, dct.get('sqlite', "data.sqlite"))
    daily_dir = os.path.join(root, dct.get("daily", "daily"))
    hdf = HDFDaily(daily_dir)
    sql_conn = sqlite3.Connection(sqlite_path)
    for key, table in dct.get("sqlite_map", {}).items():
        r[key] = LocalSqlite(sql_conn, table)
    for view in dct.get("daily_views", []):
        r[view] = hdf
    return r


def test():
    conn = sqlite3.Connection(r"D:\jaqs_fxdayu\data\data.sqlite")
    cursor = conn.cursor()
    cursor.execute("select * from sqlite_master where tbl_name = '[lb.cashFlow]';")
    print(cursor.fetchall())
    # data = pd.read_sql("schema [lb.cashFlow]", conn)
    # print(data)

def main():
    import os 
    import numpy as np
    os.chdir(r"D:\jaqs_fxdayu\data\daily")
    
    daily = HDFDaily(".")
    r = daily(fields=None, symbol=["000001.SZ", "600000.SH"], trade_date=("20160101", "20170101"))
    print(r)


if __name__ == '__main__':
    test()