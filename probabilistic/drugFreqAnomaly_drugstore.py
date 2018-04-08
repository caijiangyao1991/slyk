# -*- coding:utf8*-
import pandas as pd
import numpy as np
import json
import sys
reload(sys)
sys.setdefaultencoding('utf8')

path='./data/drugFreqAnomaly_drugstore.csv'

def readData(path):
    data = pd.read_csv(path, encoding='gb18030')
    return data

def dataAnalyse(data):
    # 计算每种药品出现次数
    data_1 = data[['comn_fst_nm_nm', 'med_cd']].groupby('med_cd').count().reset_index()
    data_1.rename(columns={'comn_fst_nm_nm': 'number'}, inplace=True)
    # 与原数据合并
    data_2 = pd.merge(data, data_1, left_on='med_cd', right_on='med_cd', how='left')
    # 过滤出现频次小于50的药品
    data_3 = data_2[data_2['number'] > 50]
    # 去掉只出现在一个药店的药
    drug = data_3['pharmacy_name'].groupby([data_3['med_cd'], data_3['pharmacy_id']]).count().reset_index()
    drugstore = drug['pharmacy_id'].groupby(drug['med_cd']).count().reset_index()
    drugstore.rename(columns={0: 'number'}, inplace=True)
    data_4 = data_3[~data_3.med_cd.isin(drugstore[drugstore['number'] == 1]['med_cd'])]

    data_fre = data_4[['pharmacy_id', 'med_cd', 'comn_fst_nm_nm']].groupby(['pharmacy_id', 'med_cd']).count().reset_index()

    # 计算阈值和均值
    data_threshold = data_fre['comn_fst_nm_nm'].groupby(data_fre['med_cd']).apply(lambda x: x.mean()+3*x.std()).reset_index()
    data_threshold = data_threshold.rename(columns={0: 'threshold'})
    data_mean = data_fre['comn_fst_nm_nm'].groupby(data_fre['med_cd']).mean().reset_index()
    data_mean = data_mean.rename(columns={'comn_fst_nm_nm' : 'mean'})

    data_5 = pd.merge(data_fre, data_threshold, left_on='med_cd', right_on='med_cd', how='left')
    data_6 = pd.merge(data_5, data_mean, left_on='med_cd', right_on='med_cd', how='left')
    data_fre_result = data_6[data_6['comn_fst_nm_nm'] > data_6['threshold']]
    data_fre_final = data_fre_result[data_fre_result['comn_fst_nm_nm'] > 100]

    for i in  range(len(data_fre_final)):
        jsonMap = {"type": u"药品销售频次异常", "common": None, "tabs": []}
        commonParar = {}
        commonParar['pharmacy_name'] = str(list(data_2[data_2['pharmacy_id']==data_fre_final.iloc[i][0]]['pharmacy_name'])[0])
        commonParar['drug_name'] = str(list(data_2[data_2['med_cd']==data_fre_final.iloc[i][1]]['comn_fst_nm_nm'])[0])
        commonParar['drugSaleFrequency'] = str(data_fre_result.iloc[i][2])
        commonParar['ThreeSigemaSaleFrequency'] = str(data_fre_result.iloc[i][3])
        commonParar['avgDrugSaleFrequency'] = str(data_fre_result.iloc[i][4])
        jsonMap['common'] = commonParar

        FigureMap = {"name": u"药品销售频次分布图", "echartsType": "bar", "echartsUnit": "%", "data": []}
        tableArray = []
        tableMap = {}
        data_tableMap= data_fre[data_fre['med_cd']==data_fre_final.iloc[i][1]]
        for j in range(len(data_tableMap)):
            tableMap['pharmacy_name'] = str(list(data_2[data_2['pharmacy_id']==data_tableMap.iloc[j][0]]['pharmacy_name'])[0])
            tableMap['drugSaleFrequency'] = str(data_tableMap.iloc[j][2])
            tableArray.append(tableMap)
            tableMap = {}
        FigureMap['data'] = tableArray
        jsonMap['tabs'].append(FigureMap)

        finalResult = json.dumps(jsonMap)
        print(finalResult)
        f = open('d://drugSaleFrequencyJson.txt', 'w')
        f.write(finalResult)
        f.close()


    print(data_fre_final.sort_values('comn_fst_nm_nm', ascending=False))

    return data_fre_result


if __name__=='__main__':
    rawdata=readData(path)
    dataAnalyse(rawdata)






