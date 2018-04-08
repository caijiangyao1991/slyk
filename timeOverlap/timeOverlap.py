# -*- coding: utf-8 -*-
"""
Created on Mon Jan 08 09:23:33 2018

@author: admin02
"""
import numpy as np
import pandas as pd
import time
import os
def file_name():
    file_dir = os.getcwd()
    l=[]
    for root ,dirs,files in os.walk(file_dir):
        for file in files:
            if os.path.splitext(file)[1] == '.csv':
                l.append(os.path.join(root,file))
    return l

# ------------------------------处理数据--------------------------------
def loadData():
    t0 = time.clock()
    pathList = file_name()
    data = pd.concat([pd.read_csv(path, encoding='gb18030', dtype={u'就诊登记号': np.str, u'患者身份证': np.str,
                                                            u'报销金额': np.float})for path in pathList],ignore_index=True)
    df1 = data[data.duplicated(subset=[u'患者身份证'], keep=False)]
    df1.rename(
        columns={u'就诊登记号': 'clineID', u'患者身份证': 'patientID', u'入院时间': 'intime', u'出院时间': 'outtime', u'报销金额': 'payment'},
        inplace=True)
    df1['intime'] = df1['intime'].apply(lambda x: pd.to_datetime(x).date())
    df1['outtime'] = df1['outtime'].apply(lambda x: pd.to_datetime(x).date())
    df2 = df1[df1[u'intime'] < df1[u'outtime']][[u'clineID', u'patientID', u'intime', u'outtime', u'payment']]
    t1 = time.clock()
    print "load data runtime：%s Seconds "%(t1-t0)
    return df2,df1
def dealData(df2):
    t0 = time.clock()
    df2=df2.sort_values(by='intime')
    df2=df2.reset_index(drop=True)
    # 在数据中完全重复的数据进行去除，我们当做一条来处理
    df2 = df2.drop_duplicates(subset=['patientID','intime','outtime','payment'])
    df3 = df2[df2.duplicated(subset=['clineID','patientID','intime','outtime'],keep=False)]
    anormyRepeat = []
    ignorRepeat = []
    for name,groupp in df3.groupby('clineID'):
        sumPayment = sum(groupp.payment)
        maxPayment = max(groupp.payment)
        if sumPayment <= 0 or sumPayment == maxPayment:
            ignorRepeat.append(name)
        else:
            anormyRepeat.append(name)
    #将具有异常的数据进行输出
    anormyData = df2[df2.clineID.isin(anormyRepeat)]
    # writer3 = pd.ExcelWriter(u'C:/Users/admin02/Desktop/repeat/anormyData/abnormal_1.xlsx')
    # anormyData.to_excel(writer3, 'page_1',encoding='gbk')
    #对符合报销金额的数据在总数据中删除，对不符合报销金额数据的重复数据在总数据中进行累加处理
    #1、在df2中删除符合报销金额条件的clineID
    df2 = df2[~df2.clineID.isin(ignorRepeat)]
    #2、对不符合报销金额的重复数据进行叠加
    df2 = df2[~df2.clineID.isin(anormyRepeat)]
    sumDf=pd.DataFrame(columns=['clineID','patientID','intime','outtime','payment'])
    for name, group in anormyData.groupby('clineID'):
        sumPayment = sum(group.payment)
        ind1 = group.index[0]
        anormyData.loc[ind1, 'payment'] = sumPayment
        sumDf = sumDf.append(anormyData.loc[[ind1]], ignore_index=True)
    #然后将数据进行合并
    df4 = pd.concat([df2,sumDf],ignore_index=True)
    #筛选出具有人重复patientID的数据
    df4=df4.sort_values(by='intime')
    df4 = df4[df4.duplicated(subset=['patientID'],keep=False)]
    df4=df4.reset_index(drop=True)
    t1 = time.clock()
    print "deal with data runtime: %s Seconds " % (t1 - t0)
    return df4
# ------------------------------时间重叠算法--------------------------------

def repeatTime(df,result):
    nlength = df.shape[0]
    for i in range(nlength):
        for j in range(i + 1, nlength):
            e1 = df.loc[i, 'outtime']
            s1 = df.loc[i, 'intime']
            s2 = df.loc[j, 'intime']
            e2 = df.loc[j, 'outtime']
            ind1 = df.loc[i, 'index']
            ind2 = df.loc[j, 'index']
            if max(s1, s2) < min(e1, e2):
                if len(result) == 0:
                    aList = set()
                    aList.add(ind1)
                    aList.add(ind2)
                    result.append(aList)
                else:
                    flag = 0
                    for k in range(len(result)):
                        if i in result[k] or j in result[k]:
                            result[k].add(ind1)
                            result[k].add(ind2)
                            flag = flag + 1
                    if flag == 0:
                        aList = set()
                        aList.add(ind1)
                        aList.add(ind2)
                        result.append(aList)
    return result

def timeOverlap1(df):
    #df来自于算法运行前
    t0 = time.clock()
    result=[]
    for name, group in df.groupby('patientID'):
        nLength = group.shape[0]
        # 将tempDf转化为所需要的格式
        tempDf = group.reset_index()
        repeatTime(tempDf,result)
    proRows = set([y for x in result for y in x])
    df1 = df.loc[proRows]
    t1 = time.clock()
    print "algorithm runtime:%s Seconds " % (t1 - t0)
    return df1

def timeOverlap2(df1):
    #df1是来自于经过时间重叠处理后的dataframe格式
    delResult=[]
    for name, group in df1.groupby('patientID'):
        sumPayment = sum(group.payment)
        maxnum = max(group.payment)
        if sumPayment == 0 or sumPayment == maxnum:
            delResult.append(name)
    #del
    resultDF = df1[~df1.patientID.isin(delResult)]
    resultDF.reset_index(drop=True)
    resultDF = resultDF.sort_values('patientID')
    return resultDF

def timeOverlap4(df,df2):
    df4 = pd.merge(df, df2, on=['clineID','patientID','intime','outtime'])
    df4.rename(columns={'patientID': u'患者身份证', 'intime': u'入院时间', 'outtime': u'出院时间'}, inplace=True)
    df4.rename(columns={'clineID': u'就诊登记号'}, inplace=True)
    #异常文件输入地址
    outputPath = os.getcwd()
    writer4 = pd.ExcelWriter(outputPath+u'/异常数据.xlsx')
    df4.to_excel(writer4, 'page_1', encoding='gbk')
if __name__ == '__main__':
    #load data
    df2,df1 = loadData()
    #deal data
    df3 = dealData(df2)
    df4 = timeOverlap1(df3)
    t2 = time.clock()
    df5 = timeOverlap2(df4)
    timeOverlap4(df1, df5)
    t3 = time.clock()
    print "data output runtime: %s Seconds " % (t3 - t2)
    print "complete!"
