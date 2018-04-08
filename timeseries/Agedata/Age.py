# -*- coding: utf-8 -*-

import MySQLdb
from datetime import datetime
from pyExcelerator import *
import numpy as np
import  pandas as pd
import matplotlib.pyplot as plt
import xlrd
import  copy




def getfeiyongData(datayy):
    """
    :return: 所有数据
    """
    con = MySQLdb.connect(host='192.168.2.134', user='root', passwd='123456', db='sync_dy', port=5029, charset='utf8')
    cur = con.cursor()
    sql = """
    SELECT a5,a7,sum(a17) as a17,sum(a23) as a23,count(*) AS cs,a6,a32 from yb_yyzyjsd
    where a7>'2014-01-01' and a17 is NOT null group by a5,a6,a7,a32"""
    cur.execute(sql)
    allData = []
    for each in cur:
        if each[0] in list(datayy[u'医院']):
            allData.append(each)
    cur.close()
    con.close()
    return allData


def parseYearAndWeek(t,T):
    """
    :param t:入院（出院）日期 String
    :return:入院（出院）的年份 和 是该年的第几周
    """

    t1 = t.split('-')
    t2 = datetime(int(t1[0]),int(t1[1]),int(t1[2]))
    week = int(t2.strftime("%U"))
    year = int(t1[0])

    T1 = T.split('-')
    T2 = datetime(int(T1[0]), int(T1[1]), int(T1[2]))
    zyday = abs((t2 - T2).days)

    return (year,week),zyday+1

#数据列分出来
def fcols(jg,a):
    rqjg=[]
    for i in range(len(jg)):
        rqjg.append(jg[i][a])
    return rqjg

def dicsum(key):
    dic = {}
    for i in range(len(key)):
        if key[i] in dic:
            dic[key[i]].append(i)
        else:
            dic[key[i]]=[i]# {hos:index[]}
    return dic

def dindexcols(lis,a,hosfydata):
    rq=[]
    for i in lis:
        rq.append(hosfydata[i][a])

    return rq

def zsum(c,b):
    data = pd.DataFrame({'sum':c,'level':b})
    combine = data['sum'].groupby(data['level'])
    d=combine.sum()
    jg=[]
    for i in range(len(d)):
        jg.append([d.index[i],d[i]])

    return jg

def zidian(lis):
    zidian={}
    for i in range(len(lis)):
        a=fcols(lis,0)
        b=fcols(lis,1)
        zidian[a[i]]=b[i]
    return zidian

def nldata(hosfydata,nl):
    hosfydatanl=[]
    for i in range(len(hosfydata)):
        if hosfydata[i][6]==nl:
            hosfydatanl.append(hosfydata[i])
    return hosfydatanl

def main():
    inputfile = "F:/yy.xlsx"
    datayy = pd.read_excel(inputfile)
    hosfeiyongdata = getfeiyongData(datayy)
    hosfydata = np.array(hosfeiyongdata)

    for i in range(len(hosfydata)):
        hosfydata[i][2]=0 if hosfydata[i][2]==None else float(hosfydata[i][2])

    for i in range(len(hosfydata)):
        hosfydata[i][3] = 0 if hosfydata[i][3] == None else float(hosfydata[i][3])


    for i in range(len(hosfydata)):
        if hosfydata[i][6]<=6:
            hosfydata[i][6]=1
        elif hosfydata[i][6]<=17 and hosfydata[i][6]>=7:
            hosfydata[i][6] = 2
        elif hosfydata[i][6]<=40 and hosfydata[i][6]>=18:
            hosfydata[i][6] = 3
        elif hosfydata[i][6] <= 65 and hosfydata[i][6] >= 41:
            hosfydata[i][6] = 4
        else:
            hosfydata[i][6] = 5

    for i in range(len(hosfydata)):
        t = str(hosfydata[i][1])
        T = str(hosfydata[i][5])
        hosfydata[i][1],hosfydata[i][5] = parseYearAndWeek(t,T)

    for i in range(len(hosfydata)):
        hosfydata[i][2]= hosfydata[i][2]/(hosfydata[i][5]+.0)
        hosfydata[i][3] = hosfydata[i][3] / (hosfydata[i][5] + .0)


    for nl in range(1,6):
        print nl
        hosfydatanl=nldata(hosfydata, nl)
        b = fcols(hosfydatanl, 1)
        c = fcols(hosfydatanl, 2)
        tcc = fcols(hosfydatanl, 3)
        jg = zsum(c, b)  # [(year,week),值]
        jg1 = zsum(tcc, b)
        jg.sort()

        yy = fcols(hosfydatanl, 0)
        yiyuan = dicsum(yy)  # {hos:index}
        hosyearweekfy={}
        hosyearweekcs={}
        hosyearweektcfy={}
        for hos in yiyuan:
            rq = dindexcols(yiyuan[hos], 1,hosfydatanl)
            fy = dindexcols(yiyuan[hos], 2,hosfydatanl)
            tcfy = dindexcols(yiyuan[hos], 3,hosfydatanl)
            cs = dindexcols(yiyuan[hos], 4,hosfydatanl)
            hosfy = zsum(fy, rq)  #[(year,week),值]
            hostcfy = zsum(tcfy, rq) #[(year,week),值]
            hoscs = zsum(cs, rq)  # [(year,week),次数]
            rqjg = fcols(jg, 0)
            hosrqjg = fcols(hosfy, 0)
            hoscsrqjg = fcols(hoscs, 0)
            hostcrqjg = fcols(hostcfy, 0)
            #补全日期
            for j in range(len(rqjg)):
                if rqjg[j] not in hosrqjg:
                    hosfy.append([rqjg[j], 0])
            for j in range(len(rqjg)):
                if rqjg[j] not in hoscsrqjg:
                    hoscs.append([rqjg[j], 0])
            for j in range(len(rqjg)):
                if rqjg[j] not in hostcrqjg:
                    hostcfy.append([rqjg[j], 0])
            hosfy.sort()
            hostcfy.sort()
            hoscs.sort()

            hosywfy=zidian(hosfy)
            hosywtcfy=zidian(hostcfy)
            hosywcs=zidian(hoscs)


            hosyearweekfy[hos] = hosywfy
            hosyearweekcs[hos] = hosywcs
            hosyearweektcfy[hos] = hosywtcfy
        hosyearweekjcfy=copy.deepcopy(hosyearweekfy)
        hosyearweekjctcfy = copy.deepcopy(hosyearweektcfy)
        for hos in hosyearweekfy:
            for week in hosyearweekfy[hos]:
                try:
                    hosyearweekjcfy[hos][week] = hosyearweekfy[hos][week]/(hosyearweekcs[hos][week]+.0)
                    hosyearweekjctcfy[hos][week] = hosyearweektcfy[hos][week] / (hosyearweekcs[hos][week] + .0)
                except ZeroDivisionError:
                    hosyearweekjcfy[hos][week]=0.0
                    hosyearweekjctcfy[hos][week] = 0.0
        #out="F:/jcfy.xlsx"
        #pd.DataFrame(hosyearweekjcfy).T.to_excel(out)

        a=pd.DataFrame(hosyearweekjcfy)

        hosstd={}
        hosstd1={}
        for hos in hosyearweekjcfy:
            hosstd[hos]=[a[hos].std(),a[hos].mean()]
            hosstd1[hos]=a[hos].std()
            for week in hosyearweekjcfy[hos]:
                if hosyearweekjcfy[hos][week] > 3*hosstd[hos][0]+hosstd[hos][1]:
                    if hosyearweekcs[hos][week]>2:
                        print hos,'均值：',hosstd[hos][1],'标准差：',hosstd[hos][0],'周:'\
                            ,week,'值:',hosyearweekjcfy[hos][week],hosyearweekcs[hos][week]

if __name__ == '__main__':
    main()










