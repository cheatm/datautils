import unittest
from datautils.fxdayu.basic import DataAPI


def assert_shape(data, shape):
    data_shape = data.shape
    assert data_shape[0] >= shape[0], (data_shape[0], shape[0])
    assert data_shape[1] >= shape[1], (data_shape[1], shape[1])


class TestAPI(unittest.TestCase):

    def init_api(self):
        return DataAPI({
            "type": "mongodb",
            "DB_MAP": {
                "FACTOR": "factors",
                "FXDAYU_FACTOR": "fxdayu_factors",
                "DAILY_INDICATOR": "SecDailyIndicator",
                "BAR": "Stock_1M",
                "DAILY": "Stock_D",
                # "BAR": "Stock_H"
            },
            "COL_MAP": {
                "API_LIST": "jz.apiList",
                "API_PARAM": "jz.apiParam",
                "INST_INFO": "jz.instrumentInfo",
                "TRADE_CAL": "jz.secTradeCal",
                "BALANCE_SHEET": "lb.balanceSheet",
                "CASH_FLOW": "lb.cashFlow",
                "FIN_INDICATOR": "lb.finIndicator",
                "INCOME": "lb.income",
                "INDEX_CONS": "lb.indexCons",
                "INDEX_WEIGHT_RANGE": "lb.indexWeightRange",
                "PROFIT_EXPRESS": "lb.profitExpress",
                "S_STATE": "lb.sState",
                "SEC_DIVIDEND": "lb.secDividend",
                "SEC_INDUSTRY": "lb.secIndustry",
                "SEC_SUSP": "lb.secSusp",
                "SEC_RESTRICTED": "lb.secRestricted",
                "WIND_FINANCE": "lb.windFinance",
                "SEC_ADJ_FACTOR": "lb.secAdjFactor"
            },
            "MONGODB_URI": "192.168.0.102,192.168.0.101"
        })

    def setUp(self):
        self.api = self.init_api()

    def test_views(self):
        income = self.api.income(symbol="000001.SZ", ann_date=("20170101",))
        assert_shape(income, (24, 87))
        balance_sheet = self.api.balance_sheet(symbol=["000001.SZ","600000.SZ"],
                                               ann_date=("20150101", "20180101"))
        assert_shape(balance_sheet, (14, 156))
        cash_flow = self.api.cash_flow(symbol=["000001.SZ","600000.SZ"], ann_date=("20150101", "20180101"))
        assert_shape(cash_flow, (46, 112))
        fin_indicator = self.api.fin_indicator(symbol=["000001.SZ","600000.SZ"], ann_date=("20160101", "20180101"))
        assert_shape(fin_indicator, (8, 162))
        index_cons = self.api.index_cons(index_code="000300.SH", in_date=(None, "20180101"), out_date=("20160101", None))
        assert_shape(index_cons, (409, 4))
        sec_susp = self.api.sec_susp(susp_date=(None, "20180101"), resu_date=("20170101", None))
        assert_shape(sec_susp, (2244, 7))
        profit_express = self.api.profit_express(ann_date=("20170701",))
        assert_shape(profit_express, (1994, 13))
        adj_factor = self.api.sec_adj_factor(symbol=["000001.SZ","600000.SZ"], trade_date=("20170101", "20180101"))
        assert_shape(adj_factor, (244, 3))
        dividend = self.api.sec_dividend(ann_date=("20170101", "20180101"))
        assert_shape(dividend, (6546, 13))

    def test_return_type(self):
        import numpy as np

        trade_cal = self.api.trade_cal()
        dtype = trade_cal.dtypes["trade_date"]
        self.assertEqual(dtype, np.int64, dtype)

        daily_indicator = self.api.daily_indicator(fields=["pe", "pb"], symbol="000001.SZ", trade_date=("20180101", None))
        dtype = daily_indicator.dtypes["trade_date"]
        self.assertEqual(dtype, np.object, dtype)

        daily = self.api.daily(["000001.SZ"], 20180101, 20180131)
        self.assertTrue("000001.SZ" in daily)
        # print(daily)

        bar = self.api.bar(["000001.SZ"], 20180103, ["datetime", "open", "close", "volume"])
        print(bar)