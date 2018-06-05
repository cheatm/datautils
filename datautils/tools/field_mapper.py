import pandas as pd
import json


def read(file_name):
    mapper = pd.read_excel(file_name)
    return mapper.groupby(mapper.windTableName).apply(create_map).to_dict()


def create_map(frame):
    return frame.set_index("factorName")["windColumnName"].to_dict()


def main():
    print(read(r"C:\Users\bigfish01\Documents\Python Scripts\datautils\name_map.xlsx"))

if __name__ == '__main__':
    main()