from datautils.fxdayu.basic import DataAPIBase, DataAPI
from datautils.fxdayu import conf


api = DataAPIBase()


def init(*configs):
    globals()["api"] = DataAPI(*configs)


def init_mongodb_example():

    init(
        {
            "type": "mongodb",
            "UPDATE": {
                "factor": "log.factor",
                "candle": "log.sinta",
                "daily_indicator": "log.dailyIndicator"
            },
            "DB_MAP": {
                "FACTOR": "factors",
                "FXDAYU_FACTOR": "fxdayu_factors",
                "DAILY_INDICATOR": "SecDailyIndicator",
                "BAR": "Stock_1M",
                "DAILY": "Stock_D"
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
            "MONGODB_URI": conf.MONGODB_URI
        }
    )    


def main():
    conf.MONGODB_URI = "192.168.0.102,192.168.0.101"
    init_mongodb_example()
    data = api.daily(["000001.SZ"], 20180101, 20180131, set())
    print(data)

if __name__ == '__main__':
    main()