import importlib


class SingleReader(object):

    def __call__(self, index=None, fields=None, **filters):
        """
        根据输入的参数返回DataFrame

        :param index: 在返回的DataFrame将指定的字段设为Index
            - None: 返回结果不设Index
            - str: 返回结果设置Index
            - list: 返回结果设置MultiIndex
        :param fields: 需要读取的字段
            - None: 全部
            - str: 单个
            - list, Iterable
        :param filters: 对每个字段的筛选条件
            - tuple: (start, end); 根据范围筛选, start或end可以为None, 代表不设最小或最大
            - str: 筛选对应字段为某一个值
            - list, set, Iterable: 筛选对应字段满足其中任何一个值的。
        :return: pandas.DataFrame
        """
        pass


class MultiReader(object):

    def __call__(self, names, index=None, fields=None, **filters):
        """
        根据输入的参数返回dict: {name: DataFrame}
        :param names: 返回的dict中对应的key
            - list
        :param index: 在返回的DataFrame将指定的字段设为Index
            - None: 返回结果不设Index
            - str: 返回结果设置Index
            - list: 返回结果设置MultiIndex
        :param fields: 需要读取的字段
            - None: 全部
            - str: 单个
            - list, Iterable
        :param filters: 对每个字段的筛选条件
            - tuple: (start, end); 根据范围筛选, start或end可以为None, 代表不设最小或最大
            - str: 筛选对应字段为某一个值
            - list, set, Iterable: 筛选对应字段满足其中任何一个值的。
        :return: dict: {name: pandas.DataFrame}
        """
        pass


class DailyReader(object):

    def __call__(self, symbols, start, end, fields=None):
        pass


class BarReader(object):

    def __call__(self, symbols, trade_date, fields=None):
        pass


class DataAPIBase(object):
    # 本地已有数据接口
    stock_d = MultiReader()
    stock_h = MultiReader()
    stock_1m = MultiReader()
    factor = SingleReader()
    fxdayu_factor = SingleReader()
    update_status = SingleReader()
    # jaqs接口
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
    daily = DailyReader()
    bar = BarReader()


class DataAPI(DataAPIBase):

    def __init__(self, *conf):
        """
        根据输入的配置初始化DataAPI
        :param conf:
            type: dict
            format:
                {"type": "module-name",
                 "other-key": "other-value",
                  ...}
        可以一次性输入多个conf。
        程序会读取每一个conf中的type, 并在datautils.fxdayu下寻找同名的module, 然后调用其中的load_conf方法, 生成对应的reader赋值个DataAPI。

        load_conf方法接收一个 conf dict 为参数, 并返回一个字典: {name: reader}
            - name: str, DataAPI的属性名
            - reader: SingleReader | MultiReader, DataAPI的属性

        可参考 datautils.fxdayu.mongodb.load_conf
        """
        self.conf = conf
        for single in conf:
            _type = single["type"]
            module = importlib.import_module("datautils.fxdayu.%s" % _type)
            readers = module.load_conf(single)
            for key, reader in readers.items():
                setattr(self, key, reader)


from functools import wraps


def single_fields_mapper(in_map, out_map):
    def wrapper(func):
        @wraps(func)
        def wrapped(index=None, fields=None, **filters):
            if isinstance(fields, (list, set)):
                fields = list(map(lambda s: in_map.get(s, s), fields))
            for key in list(filters):
                if key in in_map:
                    filters[in_map[key]] = filters.pop(key)
            print(filters)
            result = func(index, fields, **filters)
            return result.rename_axis(out_map, 1)
        return wrapped
    return wrapper