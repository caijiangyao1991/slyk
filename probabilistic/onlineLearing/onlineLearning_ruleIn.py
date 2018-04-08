# -*- coding:utf8*-

import pandas as pd
import numpy as np
import scipy.stats.stats as stats
from sklearn.feature_selection import mutual_info_classif
from sklearn.preprocessing import LabelEncoder
import datetime
from binning.monotonic_binning import  mono_bin
import sys
reload(sys)
sys.setdefaultencoding('utf8')

path = 'testdata.xlsx'
def readData(path):
    data = pd.read_excel(path, encoding='gb18030', sep="xovm02")
    return data

#变量筛选
def mutual_info(data):
    # 缺失值填补
    df = data.fillna(method = 'ffill')
    # 将时间变量转换为数值
    df['hospitalizeddate'] = ((df['hospitalizeddate'] - datetime.datetime(2008, 12, 31))/ np.timedelta64(1, 'D')).astype(int)
    df['leavedate'] = ((df['leavedate'] - datetime.datetime(2008, 12, 31)) / np.timedelta64(1, 'D')).astype(int)
    df_1 = df.copy()
    # 将字符串变量转化为数值
    str_cols = df_1.columns[df_1.dtypes == 'object']
    for col in str_cols:
        df_1[col] = LabelEncoder().fit_transform(df_1[col])
    # 计算互信息
    y = df_1['label']
    x = df_1.drop('label', axis=1)
    info = mutual_info_classif(x, y, copy=True, random_state=5)
    map = {}
    name = x.columns
    for na, info in zip(name, info):
        map[na] = info

    lista = []
    for key, value in map.iteritems():
        # print([key, value])
        if value < 0.05:
            lista.append(key)
    print(lista)
    # 去掉了与y值完全无关的变量
    df_2 = df.drop(lista, axis=1)
    return df_2


#连续变量分箱
def binning(df_2):
    df_3 = df_2.drop('label',axis=1)
    numerical_cols = df_3.columns[df_3.dtypes != 'object']
    y = df_2.label
    for cols in numerical_cols:
        x = df_2[cols]
        bin = mono_bin(y, x)
        bins = []
        for i in range(len(bin)):
            bins.append(bin.ix[i,0])
        bins.append(bin.iloc[-1, 1])
        df_2[cols] = pd.cut(x , bins)
    # print(df_2.head())
    return df_2

def prob(df_2, data):
    rule = []
    for col in df_2.columns[:-1]:
        name = df_2['label'].groupby(df_2[col]).agg(['count', 'sum']).reset_index()
        name['probility'] = name['sum']/name['count']
        for i in range(len(name)):
            if list(name['probility'])[i]<0.01 and (list(name['count'])[i])>8:
                rule.append((col,list(name[col])[i]))
    path = "rule.csv"
    f = open(path, 'w')
    for eachLine in rule:
        f.write(eachLine[0])
        f.write(" ")
        f.write(eachLine[1]+"\n")
    f.close()

    for j in rule:
        print j[0]
        print j[1]




if __name__ == "__main__":
    rawdata = readData(path)
    mutualdata = mutual_info(rawdata)
    bindata = binning(mutualdata)
    prob(bindata, rawdata)
