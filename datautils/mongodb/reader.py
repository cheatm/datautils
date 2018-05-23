from datautils.mongodb import read, parser, projection, parse_range, read_chunk
from datautils.fxdayu.basic import SingleReader, MultiReader
import logging
import pandas as pd


class ColReader(SingleReader):

    def __init__(self, collection):
        self.collection = collection

    def __call__(self, *args, **kwargs):
        return self.read(*args, **kwargs)

    def read(self, index=None, fields=None, hint=None, **filters):
        return read(self.collection, index, fields, hint, **filters)


class IndexColReader(ColReader):

    def __init__(self, collection, index):
        super(IndexColReader, self).__init__(collection)
        self.index = index

    def read(self, fields=None, start=None, end=None, **filters):
        filters[self.index] = parse_range(start, end)
        return super(IndexColReader, self).read(self.index, fields, **filters)


class DBReader(MultiReader):

    def __init__(self, db):
        self.db = db

    def __call__(self, names, index=None, fields=None, **filters):
        return dict(self.iter_read(names, index, fields, **filters))

    def read(self, names, index=None, fields=None, **filters):
        return dict(self.iter_read(names, index, fields, **filters))

    def iter_read(self, names, index=None, fields=None, **filters):
        filters = parser(**filters)
        prj = projection(index, fields)
        for name in names:
            try:
                yield name, self._read(self.get_col(name), index, filters, prj)
            except Exception as e:
                logging.error("%s | %s | %s | %s | %s", name, index, filters, prj, e)

    def get_col(self, name):
        return self.db[name]

    def _read(self, collection, index, filters, prj):
        cursor = collection.find(filters, prj)
        data = pd.DataFrame(list(cursor))
        if index is not None:
            if index in data.columns:
                return data.set_index(index)
            else:
                data.index.name = index
                return data
        else:
            return data


class MultiDBReader(DBReader):

    def __init__(self, dbs):
        super(MultiDBReader, self).__init__(dbs)
        self.dbs = {db.name: db for db in dbs}
        self.col_map = {}
        for db in self.db:
            self.col_map.update(dict.fromkeys(db.collection_names(), db))

    def get_col(self, name):
        return self.col_map[name][name]


class ChunkDBReader(DBReader):

    def _read(self, name, index, filters, prj):
        return read_chunk(self.db[name], filters, prj, index)

