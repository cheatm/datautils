import pandas as pd
import tables
from collections import Iterable
import os
from datautils.fxdayu.basic import SingleReader, DailyReader, SingleMapReader
import logging
from datautils.sql import make_command
import sqlite3
import numpy as np


class HDFStructure(object):

    index_name = "/date_flag"
    column_name="/symbol_flag"
    value_name="/data"

    @classmethod
    def rename_axis(cls, index, column):
        if index == cls.index_name and (column == cls.column_name):
            return cls
        else:
            return type("Rename_HDFStructure", (cls,), {"index_name": index, "column_name": column})

    @classmethod
    def from_root(cls, root):
        files = []
        for filename in os.listdir(root):
            
            if filename.endswith(".hd5"):
                files.append(os.path.join(root, filename))
        
        return cls.from_files(*files)
    
    @classmethod
    def from_files(cls, *files):
        structures = {}
        for path in files:
            filename = os.path.split(path)[1]
            name = filename[:-4].strip("_")
            structures[name] = cls(path)
        return structures
        
    def __init__(self, hdf_file):
        self.init(hdf_file)
        self.m_time = os.path.getmtime(self.file_name)
        self.load_indexes()
        self.file.close()

    def init(self, hdf_file):
        if isinstance(hdf_file, tables.File):
            self.file = hdf_file
            self.file_name = self.file.filename
        elif isinstance(hdf_file, str):
            self.file_name = hdf_file
            self.file = tables.File(self.file_name, "r")

    def load_indexes(self):
        self.index = self._read_index(self.index_name)
        if self.index.dtype == np.object:
            self.index = self.index.map(lambda b: int(b))
        self.column = self._read_index(self.column_name).map(lambda b: b.decode())

    def refresh(self):
        if self.file.isopen:
            self.file.close()
        self.__init__(
            self.file_name
        )

    def __del__(self):
        if self.file.isopen:
            self.file.close()

    def value(self, idx, col):
        self.init(self.file_name)
        m_time = os.path.getmtime(self.file_name)
        if m_time > self.m_time:
            self.m_time = m_time
            self.load_indexes()
        data = self.file.get_node(self.value_name)[idx, col]
        self.file.close()
        return data

    def read(self, index, columns):
        try:
            index_loc, idx = self._loc(self.index, index)
            column_loc, col = self._loc(self.column, columns)
        except Exception as e:
            logging.error("read hdf local fail | %s | %s | %s | %s", self.file_name, index, columns, e)
            return pd.DataFrame()
        data = self.value(index_loc, column_loc)
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
import os


class HDFDaily(SingleMapReader):

    @classmethod
    def mapped(cls, tag, mapper):
        return type("%s_HDF" % tag, (cls,), {"mapper": mapper})

    def __init__(self, root, cls):
        self.lock = RLock()
        self.cls = cls
        self.root = root
        self.files = self.gen_files()
        self.symbol = self.mapper.get("symbol", "symbol")
        self.date = self.mapper.get("trade_date", "trade_date")
    
    def predefine(self):
        return set(self.files)

    def gen_files(self):
        root = self.root
        Structure = self.cls
        if isinstance(root, str): 
            return Structure.from_root(root)
        elif isinstance(root, list):
            return Structure.from_files(*root)
        else:
            return {}

    def read(self, fields=None, **filters):
        symbol = filters.get(self.symbol, None)
        dates = [int(date) if date else date for date in filters.get(self.date, (None, None))]
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
        pn.major_axis.name = self.date
        pn.minor_axis.name = self.symbol
        result = pn.to_frame(False)
        for name in [self.date, self.symbol]:
            if name in result:
                result.pop(name)
        return result.reset_index()

    def refresh(self):
        logging.warning("HDF start refresh")
        self.lock.acquire()
        self.files = self.gen_files()
        self.lock.release()
        logging.warning("HDF refresh accomplish")


class HDFScanner(object):

    def __init__(self, views):
        self.running = False
        self.last_refresh_time = time()
        self.timer = Timer(1, self.loop)
        self.lock = RLock()
        self.views = views
    
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
    
    def refresh(self):
        for view_name, hdf_view in self.views.items():
            logging.warning("Refresh %s", view_name)
            hdf_view.refresh()
    

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
        data = self.reader(fields=fields, symbol=symbols, trade_date=(start, end))
        if "trade_status" not in data.columns:
            data["trade_status"] = 1
        return data


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
        conf = dct["hdf"]
        conf["map_file"] = dct.get("map_file", None)
        r.update(load_hdf(conf))
    return r


def load_sqlite(dct):
    sqlite_file = dct["file"]
    return {key: LocalSqlite(sqlite_file, table) for key, table in dct.get("table_map", {}).items()}


def load_hdf(conf):
    from datautils.tools.field_mapper import read
    if 'map_file' in conf:
        fields_map = read(conf["map_file"])
    else:
        fields_map = {}

    r = {}
    views = conf.get("views", {})
    view_map = conf.get("view_map", {})
    index = conf.get("index", "/date_flag")
    column = conf.get("column", "/symbol_flag")

    Structure = HDFStructure.rename_axis(index, column)

    for view, root in views.items():

        mapper = fields_map.get(view_map.get(view, view), {})
        if mapper:
            cls = HDFDaily.mapped(view, mapper)
        else:
            cls = HDFDaily       
        r[view] = cls(root, Structure)
    
    scanner = HDFScanner(r.copy())
    scanner.start()
    
    if conf.get("predefine", False):
        pdf = {}
        for view, method in r.items():
            pdf[view] = method.predefine
        r["predefine"] = pdf

    if "daily" in r:
        r["daily"] = DailyPrice(r.pop("daily"))
    return r    
