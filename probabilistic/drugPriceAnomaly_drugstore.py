# -* coding: utf-8 *-

import numpy as np
import pandas as pd
import sys
reload(sys)
sys.setdefaultencoding('utf8')


path1 = 'D:\git\experiment\probabilistic\data\drugPriceAno_raw.csv'
path2 = 'D:\git\experiment\probabilistic\data\drugPriceAno_price.csv'

def readdata(path1, path2):
    data = pd.read_csv(path1, encoding='gb18030')
    price = pd.read_csv(path2, encoding='gb18030')
    return data, price

def dataanalyse(data, price):
    data1 = pd.merge(data, price, left_on=['pharmacy_id','med_cd','spec'],right_on=['pharmacy_id','med_cd','spec'])
    #对每一种相同剂量的药，算出箱型图阈值点
    box1 = data1['count'].groupby([data1['med_cd'],data1['spec']]).apply(lambda x: x.mean() + 3 * (x.std())).reset_index()
    box1.rename(columns={0: 'box1'},inplace=True)
    data2 = pd.merge(data1, box1, left_on=['med_cd', 'spec'], right_on=['med_cd','spec'], suffixes=('', '_y'))

    #对同一家药店，相同药品，最高价和最低价之间不得超过20元
    box2 = data1['exam_post_uprc'].groupby([data1['pharmacy_id'],data1['med_cd'],data1['spec']]).apply(lambda x: x.max()-x.min()).reset_index()
    box2.rename(columns={0: 'box2'}, inplace=True)
    data3 = pd.merge(data2, box2, left_on=['pharmacy_id','med_cd','spec'], right_on=['pharmacy_id','med_cd','spec'])
    data4 = data3[(data3['count']>data3['box1']) & (data3['box2']>50) & (data3['count']>20)]
    print(data4.head())
    print(len(data4))
    data4.to_csv(u'E:/数联易康/2017-06/药品价格波动/final.csv',encoding='gbk')
    return data4

if __name__ == '__main__':
    data, price = readdata(path1,path2)
    dataanalyse(data, price)




