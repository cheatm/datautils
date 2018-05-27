import importlib


class SingleReader(object):

    def __call__(self, index=None, fields=None, **filters):
        pass


class MultiReader(object):

    def __call__(self, names, index=None, fields=None, **filters):
        pass


class DataAPIBase(object):

    stock_d = MultiReader()
    stock_h = MultiReader()
    stock_1m = MultiReader()
    factor = SingleReader()
    fxdayu_factor = SingleReader()
    daily_indicator = SingleReader()

    api_list = SingleReader()
    api_param = SingleReader()
    inst_info = SingleReader()
    trade_cal = SingleReader()
    balance_sheet = SingleReader()
    cash_flow = SingleReader()
    fin_indicator = SingleReader()
    income = SingleReader()
    index_cons = SingleReader()
    index_weight_range = SingleReader()
    profit_express = SingleReader()
    s_state = SingleReader()
    sec_dividend = SingleReader()
    sec_industry = SingleReader()
    sec_restricted = SingleReader()
    sec_susp = SingleReader()
    wind_finance = SingleReader()
    sec_adj_factor = SingleReader()


class DataAPI(DataAPIBase):

    def __init__(self, *conf):
        self.conf = conf
        for single in conf:
            _type = single["type"]
            module = importlib.import_module("datautils.fxdayu.%s" % _type)
            readers = module.load_conf(single)
            for key, reader in readers.items():
                setattr(self, key, reader)
