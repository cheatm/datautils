from datautils.fxdayu.basic import DataAPIBase, DataAPI
from datautils.fxdayu import conf


api = DataAPIBase()


def init(*conf):
    globals()["api"] = DataAPI(*conf)


def init_mongodb_example():
    mongodb_conf = {"type": "mongodb"}
    col_map = conf.variables()
    db_map = {key: col_map.pop(key) for key in ["FACTOR", "FXDAYU_FACTOR", "DAILY_INDICATOR", "STOCK_1M", "STOCK_D", "STOCK_H"]}
    mongodb_conf["DB_MAP"] = db_map
    mongodb_conf["COL_MAP"] = col_map
    mongodb_conf["MONGODB_URI"] = col_map.pop("MONGODB_URI")
    init(mongodb_conf)

    print(api.factor(fields=["PB", "PE"], symbol=["000001.SZ", "000002.SZ"]))

