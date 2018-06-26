from datautils.fxdayu.basic import DataAPIBase, DataAPI
from datautils.fxdayu import conf


api = DataAPIBase()


def init(*configs):
    globals()["api"] = DataAPI(*configs)