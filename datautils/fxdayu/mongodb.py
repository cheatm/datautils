from datautils.fxdayu.basic import SingleReader, DailyReader, BarReader, SingleMapReader
from datautils.mongodb.reader import ColReader, DBReader, ChunkDBReader
from datautils.mongodb import read
import pandas as pd


def expand(symbol):
    if symbol.startswith("6"):
        return symbol+".SH"
    else:
        return symbol+".SZ"


import six
from collections import Iterable


class TypeColReader(ColReader):

    _types = {}

    @classmethod
    def types(cls, name, **kwargs):
        return type(name, (cls,), {"_types": kwargs})

    def read(self, index=None, fields=None, hint=None, **filters):
        for key, value in list(filters.items()):
            if key in self._types:
                filters[key] = self.change(key, value)
        return super(TypeColReader, self).read(index, fields, hint, **filters)
    
    def change(self, key, value):
        method = self._types[key]
        if isinstance(value, str):
            return method(value)
        elif isinstance(value, Iterable):
            return value.__class__(map(method, value))
        else:
            return method(value)


class AppDBReader(SingleReader):

    def __init__(self, db, axis, index):
        self.reader = DBReader(db)
        self.axis = axis
        self.index = index
        self.db = db

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


def convert2date(string, **replace):
    if isinstance(string, six.string_types):
        return datetime.strptime(string.replace("-", ""), "%Y%m%d").replace(**replace)
    elif isinstance(string, int):
        return convert2date(str(string), **replace)
    else:
        return string


class FactorReader(SDIReader, SingleMapReader):


    def __init__(self, db, mapper=None):
        AppDBReader.__init__(self, db, "symbol", "datetime")
        SingleMapReader.__init__(self, mapper)
    
    def __call__(self, index=None, fields=None, **filters):
        return SingleMapReader.__call__(self, index, fields, **filters)

    def read(self, fields=None, **filters):
        return SDIReader.__call__(self, fields=fields, **filters)

    def input(self, fields=None, **filters):
        names, fields, filters = super(FactorReader, self).input(fields, **filters)
        if self.index in filters:
            start, end = filters[self.index]
            filters[self.index] = (convert2date(start), convert2date(end, hour=23, minute=59, second=59))
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

    def predefine(self):
        return self.db.collection_names()

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


def unfold(symbol):
    if symbol.endswith(".SH"):
        return symbol[:-3] + ".XSHG"
    elif symbol.endswith(".SZ"):
        return symbol[:-3] + ".XSHE"
    else:
        return symbol


def fold(symbol):
    if symbol.endswith(".XSHG"):
        return symbol[:6] + ".SH"
    elif symbol.endswith(".XSHE"):
        return symbol[:6] + ".SZ"
    else:
        return symbol


def date2int(date):
    return date.year*10000+date.month*100+date.day


class RenameDBReader(DBReader):

    def get_col(self, name):
        return super(RenameDBReader, self).get_col(unfold(name))


class RenameChunkReader(ChunkDBReader):

    def get_col(self, name):
        return super(RenameChunkReader, self).get_col(unfold(name))


class DailyDBReader(DailyReader):

    def __init__(self, db):
        self.reader = RenameDBReader(db)

    def __call__(self, symbols, start, end, fields=None):
        if isinstance(fields, set) and ("vwap" in fields):
            fields.add("volume")
            fields.add("turnover")
            
        dct = self.reader(symbols, "datetime", fields,
                          datetime=(convert2date(start, hour=15), convert2date(end, hour=15)))
        for value in dct.values():
            value["trade_status"] = 1
        pn = pd.Panel.from_dict(dct).rename_axis(date2int, 1)
        pn.items.name = "symbol"
        pn.major_axis.name = "trade_date"
        frame =  pn.transpose(2, 1, 0).to_frame(False)
        if isinstance(fields, set) and ("vwap" in fields):
            frame["vwap"] = frame["turnover"] / frame["volume"]
        frame["trade_status"].fillna(0, inplace=True)
        return frame.reset_index()


class BarDBReader(BarReader):

    def __init__(self, db):
        self.reader = RenameChunkReader(db)

    def __call__(self, symbols, trade_date, fields=None):
        dct = self.reader(symbols, "datetime", fields, _d=convert2date(trade_date))
        for symbol, frame in dct.items():
            frame["code"] = fold(symbol)
        pn = pd.Panel.from_dict(dct)
        pn.items.name = "symbol"
        pn = pn.transpose(2, 1, 0)
        for item, method in [("time", self.time), ("trade_date", self.date)]:
            value = list(map(method, pn.major_axis))
            pn[item] = pd.DataFrame(dict.fromkeys(pn.minor_axis, value), pn.major_axis)
        return pn.to_frame(False).reset_index(level="symbol")

    @staticmethod
    def time(dt):
        return dt.hour*10000+dt.minute*100+dt.second

    @staticmethod
    def date(dt):
        return dt.year*10000+dt.month*100+dt.day

class UpdateStatus(SingleReader):

    def __init__(self, client, trade_dates, factor="log.factor", daily_indicator="log.dailyIndicator", candle="sinta"):
        self.client = client
        self.factor = self._get_col(factor)
        self.daily_indicator = self._get_col(daily_indicator)
        self.candle = self._get_col(candle)
        self.trade_dates = trade_dates

    def __call__(self, index=None, fields=None, trade_date=(None, None), **filters):
        start, end = trade_date
        if start:
            start = int(start)
        if end:
            end = int(end)
        else:
            end = datetime.today()
            end = end.year*10000 + end.month*100 + end.day
        dates = self.trade_dates(trade_date=(start, end))
        dates = list(dates.trade_date.apply(str))

        factor_status = self.factor_status(dates[0], dates[-1])
        indicator_status = self.indicator_status(dates[0], dates[-1])
        candle_status = self.candle_status(dates)
        status = pd.DataFrame({"factor": factor_status, "daily": candle_status, "secDailyIndicator": indicator_status})
        status.fillna(0, inplace=True)
        status["status"] = status.applymap(lambda s: 1 if s != 0 else 0).prod(axis=1)
        status.index.name="trade_date"
        return status.reset_index()

    @staticmethod
    def _expand_date(date):
        if isinstance(date, str):
            return datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
        else:
            return None

    def factor_status(self, start=None, end=None):
        status = read(self.factor, index="date", date=(self._expand_date(start), self._expand_date(end)), local=2)
        return status["local"].rename_axis(lambda s: s.replace("-", ""))

    def indicator_status(self, start=None, end=None):
        status = read(self.daily_indicator, index="trade_date", trade_date=(start, end))
        return status.prod(axis=1)

    def candle_status(self, dates):
        dct = {date: self._candle_status(date) for date in dates}
        return pd.Series(dct)

    def _candle_status(self, date):
        date = self._expand_date(date)
        return self.candle.find({"date": date, "D": 1}).count()

    def _get_col(self, db_col):
        db, col = db_col.split(".", 1)
        return self.client[db][col]


from functools import partial


COL_READER_MAP = {
    "lb.secSusp": partial(RangeColReader, range_key="date", start_key="susp_date", end_key="resu_date"),
    "lb.indexCons": partial(RangeColReader, range_key="date", start_key="in_date", end_key="out_date"),
    "lb.secAdjFactor": TypeColReader.types("AdjFactorColReader", trade_date=str)
}


DB_READER_MAP = {
    "factor": FactorReader.set_default({"trade_date": "datetime"}, "UqerFactor"),
    "dyfactors": FactorReader.set_default({"trade_date": "datetime"}, "DYFactor"),
    "fxdayu.factor": FactorReader.set_default({"trade_date": "datetime"}, "FxdayuFactor"),
    "lb.secDailyIndicator": SDIReader,
    "bar": BarDBReader,
    "daily": DailyDBReader
}


def load_conf(dct):
    from pymongo import MongoClient

    client = MongoClient(dct["MONGODB_URI"])
    readers = {}
    for view, db_name in dct.get("DB_MAP", {}).items():
        db = client[db_name]
        cls = DB_READER_MAP.get(view, DBReader)
        reader = cls(db)
        if hasattr(reader, "predefine"):
            readers.setdefault("predefine", {})[view] = reader.predefine
        readers[view] = reader

    for view, db_col in dct.get("COL_MAP", {}).items():
        db, col = db_col.split(".", 1)
        collection = client[db][col]
        cls = COL_READER_MAP.get(view, ColReader)
        readers[view] = cls(collection)

    update_status = dct.get("UPDATE", None)
    if isinstance(update_status, dict):
        readers["updateStatus"] = UpdateStatus(client,
                                                readers["jz.secTradeCal"],
                                                **update_status)

    return readers


def main():
    import json
    with open(r"D:\jaqsmds\conf\mongodb-conf-local.json") as f:
        conf = json.load(f)
    readers = load_conf(conf)
    r = readers["fxdayu.factor"]
    
    print(r(fields=["L010104B"], trade_date=("20180101", "20180131"), symbol=["000001.SZ", "600000.SH"]))
