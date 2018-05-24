from datautils.fxdayu import conf
from datautils.fxdayu.basic import DataAPIBase, SingleReader
from datautils.mongodb.reader import ColReader, DBReader, ChunkDBReader
import pandas as pd


DBS = ("STOCK_D", "STOCK_H", "STOCK_1M", "FACTOR", "FXDAYU_FACTOR", "DAILY_INDICATOR")
COLS = ("API_LIST", "API_PARAM", "INST_INFO", "TRADE_CAL", "BALANCE_SHEET", "CASH_FLOW", "FIN_INDICATOR", "INCOME",
        "INDEX_CONS", "INDEX_WEIGHT_RANGE", "PROFIT_EXPRESS", "S_STATE", "SEC_DIVIDEND", "SEC_INDUSTRY", "SEC_SUSP",
        "SEC_RESTRICTED", "WIND_FINANCE", "SEC_ADJ_FACTOR")


class DataAPI(DataAPIBase):

    def __init__(self, client):
        self.client = client
        for name in COLS:
            self.__setattr__(name.lower(), self.get_col_reader(name))
        for name in DBS:
            self.__setattr__(name.lower(), self.get_db_reader(name))

    def get_col_reader(self, name):
        col = getattr(conf, name)
        Reader = COL_READER_MAP.get(name, ColReader)
        return Reader(self._gen_col(col))

    def get_db_reader(self, name):
        db = getattr(conf, name)
        Reader = DB_READER_MAP.get(name, DBReader)
        return Reader(self._gen_db(db))

    def _gen_col(self, string):
        db, col = string.split(".", 1)
        return self.client[db][col]

    def _gen_db(self, string):
        return self.client[string]

    @classmethod
    def conf(cls):
        from pymongo import MongoClient

        client = MongoClient(conf.MONGODB_URI)
        return cls(client)

    def add_fields(self, api, db):
        names = self._gen_db(db).collection_names()
        params = pd.DataFrame({"api": api, "ptype": "OUT", "must": "N"}, names)
        params.index.name = "param"
        collection = self._gen_col(conf.API_PARAM)

        from datautils.mongodb import update
        return update(collection, params)


def expand(symbol):
    if symbol.startswith("6"):
        return symbol+".SH"
    else:
        return symbol+".SZ"


import six
from collections import Iterable


class AppDBReader(SingleReader):

    def __init__(self, db, axis, index):
        self.reader = DBReader(db)
        self.axis = axis
        self.index = index

    def __call__(self, index=None, fields=None, **filters):
        names, fields, filters = self.input(fields, **filters)
        data = self.reader(names, self.index, fields, **filters)
        data = self.output(data)
        if index:
            return data.set_index(index)
        else:
            return data

    def input(self, fields=None, **filters):
        return fields, filters.pop(self.axis, None), filters

    def output(self, data):
        return data


class SDIReader(AppDBReader):

    def __init__(self, db):
        super(SDIReader, self).__init__(db, "symbol", "trade_date")

    def output(self, data):
        data = pd.Panel.from_dict(data)
        data.minor_axis.name = self.axis
        data.major_axis.name = self.index
        data.rename_axis(expand, 2, inplace=True)
        data = data.to_frame(False)
        for name in ("symbol", "trade_date"):
            if name in data.columns:
                data.pop(name)
        return data.sortlevel(1).reset_index()

    def input(self, fields=None, **filters):
        names, fields, filters = super(SDIReader, self).input(fields, **filters)
        if isinstance(fields, six.string_types):
            fields = fields[:6]
        elif isinstance(fields, Iterable):
            fields = list(map(lambda s: s[:6], fields))
        return names, fields, filters


from datetime import datetime


def str2date(string, **replace):
    if isinstance(string, six.string_types):
        return datetime.strptime(string.replace("-", ""), "%Y%m%d").replace(**replace)
    else:
        return string


class FactorReader(SDIReader):

    def __init__(self, db):
        AppDBReader.__init__(self, db, "symbol", "datetime")

    def input(self, fields=None, **filters):
        names, fields, filters = super(FactorReader, self).input(fields, **filters)
        if self.index in filters:
            start, end = filters[self.index]
            filters[self.index] = (str2date(start), str2date(end, hour=23, minute=59, second=59))
        return names, fields, filters

    def output(self, data):
        data = pd.Panel.from_dict(data)
        data.minor_axis.name = self.axis
        data.major_axis.name = "trade_date"
        data.rename_axis(expand, 2, inplace=True)
        data.rename_axis(lambda t: t.strftime("%Y%m%d"), 1, inplace=True)
        data = data.to_frame(False)
        for name in ("symbol", "trade_date"):
            if name in data.columns:
                data.pop(name)
        return data.sortlevel(1).reset_index()


class RangeColReader(ColReader):

    def __init__(self, collection, range_key, start_key, end_key):
        super(RangeColReader, self).__init__(collection)
        self.rk = range_key
        self.start_key = start_key
        self.end_key = end_key

    def read(self, index=None, fields=None, hint=None, **filters):
        rk = filters.pop(self.rk, None)
        if isinstance(rk, tuple):
            start, end = rk[0], rk[1]
            if start:
                filters["$or"] = [{self.end_key: {"$gte": start}}, {self.end_key: ""}]
            if end:
                filters[self.start_key] = {"$lte": end}
        return super(RangeColReader, self).read(index, fields, hint, **filters)


from functools import partial


COL_READER_MAP = {
    "SEC_SUSP": partial(RangeColReader, range_key="date", start_key="susp_date", end_key="resu_date"),
    "INDEX_CONS": partial(RangeColReader, range_key="date", start_key="in_date", end_key="out_date")
}


DB_READER_MAP = {
    "FACTOR": FactorReader,
    "FXDAYU_FACTOR": FactorReader,
    "DAILY_INDICATOR": SDIReader,
    "STOCK_1M": ChunkDBReader
}