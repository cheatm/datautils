from datautils.fxdayu import conf
from datautils.fxdayu.basic import DataAPIBase
from datautils.mongodb.reader import ColReader, DBReader


class DataAPI(DataAPIBase):

    def __init__(self, client):
        self.client = client
        for name, col in conf.COLS.items():
            self.__setattr__(name.lower(), ColReader(self._gen_col(col)))
        for name, db in conf.DBS.items():
            self.__setattr__(name.lower(), DBReader(self._gen_db(db)))

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

