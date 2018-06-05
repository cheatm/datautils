import pandas as pd
import tables
from collections import Iterable
import os
from datautils.fxdayu.basic import SingleReader, DailyReader
import logging
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

    def refresh(self):
        if self.file.isopen:
            self.file.close()
        self.__init__(
            self.file_name,
            self.value_name,
            self.index_name,
            self.column_name
        )

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
        try:
            index_loc, idx = self._loc(self.index, index)
            column_loc, col = self._loc(self.column, columns)
        except Exception as e:
            logging.error("read hdf locate fail | %s | %s | %s | %s", self.file_name, index, columns, e)
            return pd.DataFrame()
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
            if not isinstance(indexer, Iterable):
                indexer = [indexer]
            return indexer, index[indexer]

    @staticmethod
    def _iter_loc(index, loc):
        for name in loc:
            try:
                yield index.get_loc(name)
            except:
                pass


from threading import Timer, RLock
from time import time


class HDFDaily(SingleReader):

    def __init__(self, root, index, column, exclude=tuple()):
        self.root = root
        self.files = {}
        for filename in os.listdir(self.root):
            if filename.endswith(".hd5"):
                name = filename[:-4].strip("_")
                v_name = "/" + name
                self.files[name] = HDFStructure(os.path.join(self.root, filename), v_name, index, column)
        for name in exclude:
            s = self.files.pop(name, None)
            if s:
                s.file.close()
        self.running = False
        self.last_refresh_time = time()
        self.timer = Timer(1, self.loop)
        self.lock = RLock()
    
    def start(self):
        self.running = True
        self.timer.start()
    
    def stop(self):
        self.running = False
    
    def loop(self):
        if self.running:
            if time() - self.last_refresh_time >= 3600:
                self.refresh()
                self.last_refresh_time = time()
            self.timer = Timer(1, self.loop)
            self.timer.start()
        else:
            self.timer.cancel()

    def __call__(self, index=None, fields=None, **filters):
        symbol = filters.get("symbol", None)
        dates = filters.get("trade_date", (None, None))
        dates = slice(*dates)
        if fields is None:
            fields = list(self.files.keys())
        elif isinstance(fields, str):
            fields = [fields]
        
        self.lock.acquire()
        dct = {}
        for field in fields:
            try:
                struct = self.files[field]
            except KeyError:
                logging.error("hdf read | %s | %s | %s | field not exists", field, dates, symbol)
                continue
            try:
                data = struct.read(dates, symbol)
            except Exception as e:
                logging.error("hdf read | %s | %s | %s | %s", field, dates, symbol, e)
                dct[field] = pd.DataFrame()
            else:
                dct[field] = data
        self.lock.release()
        pn = pd.Panel.from_dict(dct)
        pn.major_axis.name = "trade_date"
        pn.minor_axis.name = "symbol"
        return pn.to_frame(False).reset_index()

    def refresh(self):
        logging.warning("HDF start refresh")
        self.lock.acquire()
        for structure in self.files.values():
            structure.refresh()
        self.lock.release()
        logging.warning("HDF refresh accomplish")

class DailyPrice(DailyReader):

    limits = {"open", "high", "low", "close", "volume", "turnover", "vwap"}

    def __init__(self, reader):
        self.reader = reader
        
    def __call__(self, symbols, start, end, fields=None):
        if fields is None:
            fields = self.limits
        elif isinstance(fields, Iterable) and not isinstance(fields, str):
            fields = self.limits.intersection(set(fields))
        else:
            fields = self.limits.intersection({fields})
        return self.reader(fields=fields, symbol=symbols, trade_date=(str(start), str(end)))


class LocalSqlite(SingleReader):

    def __init__(self, sqlite_file, table):
        assert os.path.isfile(sqlite_file)
        self.file = sqlite_file
        self.table = table
        self.columns = self.get_columns()
    
    def get_columns(self):
        conn = sqlite3.Connection(self.file)
        cursor = conn.cursor()
        cursor.execute("select * from %s" % self.table)
        return set([record[0] for record in cursor.description])

    def __call__(self, index=None, fields=None, **filters):
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
    r = {}
    if "sqlite" in dct:
        r.update(load_sqlite(dct["sqlite"]))
    if "hdf" in dct:
        r.update(load_hdf(dct["hdf"]))
    return r


def load_sqlite(dct):
    sqlite_file = dct["file"]
    return {key: LocalSqlite(sqlite_file, table) for key, table in dct.get("table_map", {}).items()}


def load_hdf(dct):
    r = {}
    root = dct.get("root", "daily")
    hdf = HDFDaily(root, dct.get("index", "/date_flag"), dct.get("column", "/symbol_flag"), dct.get("exclude", []))
    hdf.start()
    views = set(dct.get("views", []))
    if "daily" in views:
        views.remove("daily")
        r["daily"] = DailyPrice(hdf)
    for view in views:
        r[view] = hdf
    return r    

