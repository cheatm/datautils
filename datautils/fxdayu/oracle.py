import cx_Oracle
import pandas as pd
from datautils.fxdayu.basic import SingleReader, MultiReader


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
    fields = ",".join(fields)
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
                yield "%s>=%s" % (key, start)
            if end:
                yield "%s<=%s" % (key, end)
        else:
            if isinstance(value, str):
                value = "'%s'" % value
            yield "%s = %s" % (key, value)


def read_oracle(cursor, name, fields, **filters):
    command = make_command(name, fields, **filters)
    try:
        cursor.execute(command)
        r = cursor.fetchall()
    except Exception as e:
        print(e)
        return pd.DataFrame(columns=fields)
    else:
        return pd.DataFrame(r, columns=fields)


class OracleSingleReader(SingleReader):

    DATE = "TDATE"
    SYMBOL = "SYMBOLCODE"
    VALUE = "RAWVALUE"
    COLUMNS = [DATE, SYMBOL, VALUE]
    INDEX = [SYMBOL, DATE]

    def __init__(self, conn):
        self.conn = conn

    def __call__(self, index=None, fields=None, **filters):
        cursor = self.conn.cursor()
        for key in list(filters):
            if key in FIELDS_MAP:
                filters[FIELDS_MAP[key]] = filters.pop(key)

        dct = {field: self._read(cursor, FIELDS_MAP.get(field, field), **filters) for field in fields}
        return pd.DataFrame(dct).reset_index().rename_axis(REVERSED_FIELDS_MAP, 1)

    def _read(self, cursor, field, **filters):
        data = read_oracle(cursor, field, self.COLUMNS, **filters)
        return data.set_index(self.INDEX)[self.VALUE]


def load_conf(dct):

    return {}

if __name__ == '__main__':
    connection = cx_Oracle.Connection("FXDAYU/Xinger520@192.168.0.102:1520/xe")
    reader = OracleSingleReader(connection)
    r = reader(fields=["A020006A", "B010007A"], symbol="000616.SZ", trade_date=("20160104", "20160131"))
    print(r)