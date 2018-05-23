from datautils.fxdayu.basic import DataAPIBase
from datautils.fxdayu.mongodb import DataAPI


api = DataAPIBase()


def init(uri=None):
    if uri:
        from pymongo import MongoClient
        client = MongoClient(uri)
        globals()["api"] = DataAPI(client)
    else:
        globals()["api"] = DataAPI.conf()

if __name__ == '__main__':
    init("192.168.0.102")

    print(api.add_fields("factor", "fxdayu_factors"))
