from datautils.mongodb import read, parser, projection, parse_range
from datautils.fxdayu.basic import SingleReader, MultiReader
import logging
import pandas as pd
import six


class ColReader(SingleReader):

    def __init__(self, collection):
        self.collection = collection

    def __call__(self, *args, **kwargs):
        return self.read(*args, **kwargs)

    def read(self, index=None, fields=None, **filters):
        return read(self.collection, index, fields, **filters)


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

    def __call__(self, *args, **kwargs):
        return self.read(*args, **kwargs)

    def read(self, names, index=None, fields=None, **filters):
        filters = parser(**filters)
        prj = projection(index, fields)
        if isinstance(names, six.string_types):
            names = names.split(",")
        return dict(self.iter_read(names, index, filters, prj))

    def iter_read(self, names, index, filters, prj):
        for name in names:
            try:
                yield name, self._read(name, index, filters, prj)
            except Exception as e:
                logging.error("%s | %s | %s | %s | %s", name, index, filters, prj, e)


    def _read(self, name, index, filters, prj):
        cursor = self.db[name].find(filters, prj)
        data = pd.DataFrame(list(cursor))
        if index is not None:
            return data.set_index(index)
        else:
            return data
