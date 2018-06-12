import pandas as pd
import numpy as np
import json


def read(file_name):
    mapper = pd.read_excel(file_name)
    return mapper.groupby(mapper.windTableName).apply(create_map).to_dict()


def create_map(frame):
    return frame.set_index("factorName")["windColumnName"].to_dict()


def main():
    print(np.datetime64)
    # print(read(r"C:\Users\bigfish01\Documents\Python Scripts\datautils\name_map.xlsx"))
    data = pd.DataFrame("a", pd.date_range("2018-01-01", "2018-01-31"), columns=["a", "c", "d"])
    data["b"] = 0
    data.index.name="date"
   
    print(data.reset_index().select_dtypes(include=[np.object, np.number, np.datetime64]))
