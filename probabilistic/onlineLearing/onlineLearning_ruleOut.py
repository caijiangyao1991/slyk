# -*- coding:utf8*-

import pandas as pd
import numpy as np
import csv

def readrule():
    data = pd.read_excel('testdata.xlsx', encoding='gb18030', sep="xovm02")
    f = open('rule.csv', 'rb')
    reader = csv.reader(f)
    for line in reader:
        name = line[0].split(" ")[0]
        value = line[0].split(" ")[1]
        data[data[name]== value]['lable'] = 0


    print(data.head())





if __name__ == "__main__":
    readrule()