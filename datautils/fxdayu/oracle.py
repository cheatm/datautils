import cx_Oracle
import pandas as pd
from datautils.fxdayu.basic import SingleReader, MultiReader, SingleMapReader
from datetime import datetime
# from datautils.sql import make_command
import logging


FIELDS_MAP = {
    "symbol": "SYMBOLCODE",
    "trade_date": "TDATE",
}


def reverse_map(dct):
    return {value: key for key, value in dct.items()}


REVERSED_FIELDS_MAP = reverse_map(FIELDS_MAP)


def make_command(name, fields, **filters):
    template = "select %s from %s"
    where = "%s where %s"
    fields = ",".join(fields) if fields else "*"
    command = template % (fields, name)
    if len(filters) == 0:
        return command
    else:
        return where % (command, " and ".join(iter_filters(**filters)))


def iter_filters(**filters):
    for key, value in filters.items():
        if isinstance(value, (set, list)):
            yield "%s in %s" % (key, tuple(value))
        elif isinstance(value, tuple):
            start, end = value[0], value[1]
            if start:
                yield "%s>=%s" % (key, convert(start))
            if end:
                yield "%s<=%s" % (key, convert(end))
        else:
            if isinstance(value, str):
                value = "'%s'" % value
            yield "%s=%s" % (key, convert(value))


def convert(value):
    if isinstance(value, datetime):
        return "TO_DATE('%s', 'YYYY-MM-DD')" % value.strftime("%Y-%m-%d")
    else:
        return value

def read_oracle(cursor, name, fields, **filters):
    command = make_command(name, fields, **filters)
    # print(command)
    try:
        cursor.execute(command)
        r = cursor.fetchall()
    except Exception as e:
        print(e)
        return pd.DataFrame(columns=fields)
    else:
        return pd.DataFrame(r, columns=fields)


class OracleSingleReader(SingleMapReader):

    DATE = "TDATE"
    SYMBOL = "SYMBOLCODE"
    VALUE = "RAWVALUE"
    DTYPE = "PROCESSEDTYPE"
    DEFAULT_DTYPE = "A" 

    def __init__(self, conn, db):
        self.conn = conn
        self.db = db
    
    def get_tables(self):
        command = make_command("ALL_TABLES", ["TABLE_NAME"], OWNER=self.db)
        tables = pd.read_sql(command, self.conn)
        return set(tables["TABLE_NAME"])

    def read(self, fields=None, **filters):
        if fields is None:
            fields = self.get_tables()
        cursor = self.conn.cursor()
        dct = dict(self._iter_read(cursor, fields, filters))
        return pd.DataFrame(dct).reset_index()
    
    def _iter_read(self, cursor, fields, filters):
        for field in fields :     
            try: 
                yield field, self._read(cursor, field, **filters)
            except Exception as e:
                logging.error("read Oracle | %s | %s | %s | %s", self.db, field, filters, e)
                

    def _read(self, cursor, field, **filters):
        real_field = field.split("-", 1)
        if len(real_field)  == 2:
            field = real_field[0]
            filters[self.DTYPE] = real_field[1]
        else:
            filters[self.DTYPE] = self.DEFAULT_DTYPE
        field = ".".join((self.db, field))
        data = read_oracle(cursor, field, self.COLUMNS, **filters)
        return data.drop_duplicates(self.INDEX).set_index(self.INDEX)[self.VALUE]

    @property
    def COLUMNS(self):
        return [self.DATE, self.SYMBOL, self.VALUE]
    
    @property
    def INDEX(self):
        return [self.SYMBOL, self.DATE]


class OracalTableReader(SingleMapReader):

    def __init__(self, conn, table):
        self.conn = conn
        self.table = table
    
    def read(self, fields=None, **filters):
        if isinstance(fields, str):
            fields = {fields}
        command = make_command(self.table, fields, **filters)
        return pd.read_sql(command, self.conn)


def load_conf(dct):
    r = {}
    url = dct["url"]
    connection =  cx_Oracle.Connection(url)
    for api, conf in dct.get("api", {}).items():
        db = conf["db"]
        ReaderCls = get_reader_cls(api, conf)
        reader = ReaderCls(connection, db)
        r[api] = reader
        if conf.get("predefine", False):
            r.setdefault("predefine", {})[api] = reader.get_tables

    return r


def get_reader_cls(name, dct):
    fields_map = dct.get("fields_map", FIELDS_MAP)
    table_structure = dct.get("table_structure", {})
    attrs = {}
    attrs["mapper"] = fields_map
    attrs.update(table_structure)
    return type("%sReader" % name, (OracleSingleReader,), attrs)


if __name__ == "__main__":
    # connection =  cx_Oracle.Connection("bigfish/bigfish@172.16.55.54:1521/ORCL")
    # reader = OracalTableReader(connection, "ZYYX2.CON_FORECAST_STK")
    # print(reader(STOCK_CODE="603355"))
    r = make_command("ZYYX2.CON_FORECAST_STK", None, STOCK_CODE="603355", CON_DATE=(datetime(2018, 1, 1), None))
    print(r)