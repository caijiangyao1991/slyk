# -*- coding: utf-8 -*-

import MySQLdb
import psycopg2
import math
import numpy as np
import pandas as pd
from pandas import DataFrame
from datetime import datetime
from dtw import dtw
from numpy import array
from numpy.linalg import norm
import matplotlib.pyplot as pl
import copy
import json
import sys
import datetime
reload(sys)
sys.setdefaultencoding('utf8')

HISCIRCLE = 27
EACHCIRCLE = 7
HOSLEVEL = 3


def dis(a, b):
    x = array(a).reshape(-1, 1)
    y = array(b).reshape(-1, 1)
    dist, cost, acc, path = dtw(x, y, dist=lambda x, y: norm(x - y))
    return dist

def runnian(year):
    flage = False
    if (year % 4) == 0:
        if (year % 100) == 0:
            if (year % 400) == 0:
                flage=True  # 整百年能被400整除的是闰年
        else:
            flage = True  # 非整百年能被4整除的为闰年
    return flage

def getRydata(hoses):
    sql ="""
          SELECT t.hosId, t.inHosIime,COUNT(*) AS cnt
           FROM (
         SELECT KC21_AAZ217,extract(YEAR from KC21_CKC537)||'-'||extract(doy from KC21_CKC537)-1 as inHosIime,KB01_CKZ543 as hosId
         FROM t_jsxx_bc
         WHERE KB01_AKA101='03'  AND KC21_CKC537 is not null AND KC21_CKC538>=KC21_CKC537
       AND to_char(KC21_CKC537 ,'YYYY-MM') >= '2014-01'  AND   to_char(KC21_CKC537 ,'YYYY-MM') <= '2016-12'  AND KB01_CKZ543 IS NOT NULL
       ) t
          GROUP BY t.hosId,t.inHosIime
    """
    try:
        rydata= {}
        con = psycopg2.connect(host='192.168.10.60', port=5432, user='gpadmin', password='postgres', database='dy')
        cur = con.cursor()
        cur.execute(sql)
        for each in cur:
            #print each
            hos = each[0]
            if hos not in rydata:
                rydata[hos] = {}
                for eachYear in [2014, 2015, 2016]:
                    tmp = []
                    if runnian(eachYear):
                        for day in range(366):
                            tmp.append(0)
                    else:
                        for day in range(365):
                            tmp.append(0)
                    rydata[hos][eachYear] = tmp
            yearAndDay = [int(i) for i in each[1].split("-")]
            rydata[hos][yearAndDay[0]][yearAndDay[1]-1] = int(each[2])
        #print u'ry', rydata.keys()
        for hos in hoses:
            if hos not in rydata:
                rydata[hos] = {}
                for eachYear in [2014, 2015, 2016]:
                    tmp = []
                    if runnian(eachYear):
                        for day in range(366):
                            tmp.append(0)
                    else:
                        for day in range(365):
                            tmp.append(0)
                    rydata[hos][eachYear] = tmp
    except Exception,e:
        print e
        cur.close()
        con.close()
        return None
    cur.close()
    con.close()

    #print u'ry', rydata.keys()
    return rydata

def getCydata(hoses):

    sql = """
             SELECT T.hosId, T.outHosIime,COUNT (*) AS cnt
        FROM(SELECT KC21_AAZ217,extract(YEAR from KC21_CKC538)||'-'||extract(doy from KC21_CKC538)-1 AS outHosIime,
            KB01_CKZ543 AS hosId FROM t_jsxx_bc
        WHERE KB01_AKA101 = '03' AND KC21_CKC538 IS NOT NULL AND KC21_CKC538 >= KC21_CKC537 AND to_char(KC21_CKC538,'YYYY-MM') >= '2014-01' AND to_char(KC21_CKC538,'YYYY-MM') <= '2016-12'
        AND KB01_CKZ543 IS NOT NULL) T
        GROUP BY T.hosId, T.outHosIime
    """
    try:
        cydata= {}
        con = psycopg2.connect(host='192.168.10.60', port=5432, user='gpadmin', password='postgres', database='dy')
        cur = con.cursor()
        cur.execute(sql)
        for each in cur:
            hos = each[0]
            if hos  not in cydata:
                cydata[hos] = {}
                for eachYear in [2014,2015,2016]:
                    tmp=[]
                    if runnian(eachYear):
                        for day in range(366):
                            tmp.append(0)
                    else:
                        for day in range(365):
                            tmp.append(0)
                    cydata[hos][eachYear] = tmp

            yearAndDay =[int(i) for i in  each[1].split("-")]
            cydata[hos][yearAndDay[0]][yearAndDay[1]-1] = int(each[2])
        #print u'cy', cydata.keys()
        for hos in hoses:
            if hos not in cydata:
                cydata[hos] = {}
                for eachYear in [2014, 2015, 2016]:
                    tmp = []
                    if runnian(eachYear):
                        for day in range(366):
                            tmp.append(0)
                    else:
                        for day in range(365):
                            tmp.append(0)
                    cydata[hos][eachYear] = tmp
    except Exception,e:
        print e
        cur.close()
        con.close()
        return None
    cur.close()
    con.close()
    #print u'cy', cydata.keys()
    return cydata

def initZyData(hoses):
    zydata = {}
    for hos in hoses:
        zydata[hos] = {}
        for eachYear in [2014, 2015, 2016]:
            tmp = []
            if runnian(eachYear):
                for day in range(366):
                    tmp.append(0)
            else:
                for day in range(365):
                    tmp.append(0)
            zydata[hos][eachYear] = tmp

    return zydata

def getZyData(rydata, cydata, initHos):
    hoses = initHos.keys()
    zydata = initZyData(hoses)
    years = [2014, 2015, 2016]
    days = range(365)

    for year in years:
        if runnian(year):
            days = range(366)
        else:
            days = range(365)
        for day in days:
            for hos in hoses:
                if 2014 == year and 0 == day:

                    zydata[hos][year][day] = initHos[hos] + rydata[hos][year][day]

                else:
                    if (2015 == year and 0 == day) or (2016 == year and 0 == day):
                        zydata[hos][year][day] = zydata[hos][year-1][364] - cydata[hos][year-1][364] + rydata[hos][year][day]
                    else:
                        zydata[hos][year][day] = zydata[hos][year][day-1] - cydata[hos][year][day-1] + rydata[hos][year][day]


    for hos in hoses:
        print hos#, zydata[hos]

    return zydata

def getCircleData(rydata,zydata,hos,year,day):
    curYear = year
    curDay = day
    eachCircle = HISCIRCLE
    tmp = []
    sumZyData = 0
    while eachCircle > 0:
        pastDay = curDay-6
        if pastDay >= 0:
            # print rydata[hos][curYear][pastDay:curDay+1] ,
            sumZyData += sum(zydata[hos][curYear][pastDay:curDay+1])
            tmp.append(zydata[hos][curYear][pastDay] + sum(rydata[hos][curYear][pastDay+1:curDay+1]))
        else:
            pastDay += 365
            # print pastDay
            # print curYear-1
            # print zydata[hos][curYear-1][pastDay] , sum(rydata[hos][curYear-1][pastDay+1:365]) , sum(rydata[hos][curYear][0:curDay+1])

            # print rydata[hos][curYear-1][pastDay:365],rydata[hos][curYear][0:curDay+1]

            sumZyData += sum(zydata[hos][curYear-1][pastDay:365]) + sum(zydata[hos][curYear][0:curDay+1])
            tmp.append(zydata[hos][curYear-1][pastDay] + sum(rydata[hos][curYear-1][pastDay+1:365]) + sum(rydata[hos][curYear][0:curDay+1]))

        curDay = curDay - 7
        if curDay < 0:
            curDay += 365
            curYear -= 1
        eachCircle -= 1
    # print
    tmp.reverse()
    return tmp,sumZyData


def getInitHos():
    zysql ="""
                SELECT tp.hosId,COUNT(*) AS cnt
          FROM (
        SELECT tjb.KC21_AAZ217,KB01_CKZ543 as hosId
          FROM t_jsxx_bc tjb
          WHERE to_char(KC21_CKC538,'YYYY-MM-DD') >= '2013-12-31' and to_char(KC21_CKC537,'YYYY-MM-DD') <= '2013-12-31'  AND tjb.KB01_CKZ543 IS NOT NULL and tjb.KB01_AKA101='03'

        )tp
          GROUP BY tp.hosId
    """
    con = psycopg2.connect(host='192.168.10.60', port=5432, user='gpadmin', password='postgres', database='dy')
    cur = con.cursor()
    cur.execute(zysql)

    hosZyData = {}
    for eachData in cur:
        hosZyData[eachData[0]] = int(eachData[1])

    cur.close()
    con.close()

    hosCyData = {}
    cysql = """
                 SELECT ttp.hosId,COUNT(*) AS cnt
            FROM (
          SELECT tjb.KC21_AAZ217,KB01_CKZ543 as hosId
            FROM t_jsxx_bc tjb
            WHERE to_char(KC21_CKC538,'YYYY-MM-DD') = '2013-12-31'  AND tjb.KB01_CKZ543 IS NOT NULL and tjb.KB01_AKA101='03'
          ) ttp
            group BY ttp.hosId

    """

    con = psycopg2.connect(host='192.168.10.60', port=5432, user='gpadmin', password='postgres', database='dy')
    cur = con.cursor()
    cur.execute(cysql)
    for eachData in cur:
        hosCyData[eachData[0]] = int(eachData[1])

    cur.close()
    con.close()
    initHos ={}
    for hos in hosZyData:
        initHos[hos] = hosZyData[hos] - hosCyData.get(hos,0)

    return initHos



def getBigTenHos(zydata):
    checkYearAndDay ={2015:range(0,365),2016: range(0, 366)}
    hosBigTen = []
    for hos in zydata:
        flag = False
        for year in checkYearAndDay:
            for day in checkYearAndDay[year]:
                if zydata[hos][year][day] < 10:
                    flag = True
        if flag == False:
            hosBigTen.append(hos)

    return hosBigTen

def getBigTenHos1(zydata):

    #checkYearAndDay ={2015:range(340,365), 2016:range(0,30)}
    checkYearAndDay = {2014: range(188, 365, 7), 2015: range(0, 365), 2016: range(0, 30)}

    hosBigTen = []
    for hos in zydata:
        flag = False
        for year in checkYearAndDay:
            for ind, day in enumerate(checkYearAndDay[year]):
                lenDay = 365-day-1
                lastDay = checkYearAndDay[year][ind-1]+1
                if lenDay < 7 and lenDay > 0:
                    weekCnt = sum(zydata[hos][year][day+1:365])+sum(zydata[hos][year+1][0:7-lenDay])
                    checkYearAndDay[year+1] = range(7-lenDay, 365, 7) if year+1 != 2016 else range(7-lenDay, 30, 7)
                    if weekCnt < 10:
                        flag = True
                else:
                    weekCnt = sum(zydata[hos][year][lastDay:day+1])
                    if weekCnt < 10:
                        flag = True
        if flag == False:
            hosBigTen.append(hos)

    return hosBigTen



def getHosName():
    sql = """
    SELECT dh.KB01_CKZ543,dh.KB01_CKB519
  FROM d_hospital dh
  WHERE dh.KB01_AKA101='03'
    """
    con = psycopg2.connect(host='192.168.10.60', port=5432, user='gpadmin', password='postgres', database='dy')

    cur = con.cursor()
    cur.execute(sql)
    hosName = {}
    for eachData in cur:
        hosName[eachData[0]] = eachData[1]

    cur.close()
    con.close()
    return hosName

def main():

    hoslevel = {1:u"社区未定级",2:u"未定级医院",3:u"一级医院",4:u"二级乙等医院",5:u"二级甲等医院",6:u"三级乙等医院",7:u"三级甲等医院"}
    # hosName= {'372':	u'江安县人民医院','270': u'宜宾市工人医院','256':	u'珙县人民医院','229':	u'宜宾市矿山急救医院','292':	u'南溪区人民医院','362':	u'南溪区中医院','275':	u'宜宾县人民医院','258':	u'宜宾市第二中医医院','264':	u'宜宾市第三人民医院','569':	u'江安县中医医院','589':	u'长宁县人民医院','590':	u'长宁县中医医院','600':	u'高县人民医院','618':	u'筠连县人民医院','278':	u'珙县中医院'}
    # initHos = dict((('2620',57),('2621',173),('2574',131),('2510',282),('2590',46),('2479',40),('2784',65),('2494',43),('2825',40),('2439',74),('2734',36),('2756',41),('2755',54),('2766',72),('2616',71)))
    # initHos = dict((('618',66),('372',75),('270',37),('229',275),('256',43),('275',69),('264',170),('292',130),('362',45),('589',52),('258',56),('600',71),('569',36),('278',40),('590',41)))
    hosName = getHosName()
    initHos = getInitHos()


    rydata = getRydata(initHos)
    cydata = getCydata(initHos)

    zydata = getZyData(rydata, cydata, initHos)

    hosBigTen = getBigTenHos(zydata)
    print hosBigTen

    # circleYearsAndDays = {2014:range(188,365) ,2015:range(0,280)}
    # circleYearsAndDays = {2015:range(0,280)}
    # circleYearsAndDays = {2015:[5]}

    #fp = open(r'F:\level5.csv', 'w')
    #head = u"医院代码;医院名称;医院级别;发生时间;当前天在院人数;当前周在院人数;前一周在院人数;历史平均在院人数;增长率;同级医院增长率"
    #fp.write(head)
    #fp.write('\n')
    #2015:range(0,365),
    #circleYearsAndDays = {2015:range(340,365),2016:range(0,30)}
    circleYearsAndDays = {2015: range(188, 365, 7), 2016: range(6, 366, 7)}
    years = [2015, 2016]
    j = 0
    for year in years:
        print year
        start = datetime.datetime(year, 1, 1)
        for day in circleYearsAndDays[year]:
            delta = datetime.timedelta(days=day)
            #print year, day
            slideHosData = {}
            hosSumZydata = {}
            for hos in hosBigTen:
                tmphos ,sumZyData= getCircleData(rydata, zydata, hos, year, day)
                slideHosData[hos] = tmphos
                hosSumZydata[hos] = sumZyData


            allDataFrame = DataFrame(slideHosData.values())
            allData = allDataFrame.sum(axis=0)

            avgAgeNums = list(allData.values)
            #print avgAgeNums
            avgAgeRatio = []
            for ind,va in enumerate(avgAgeNums[:-1]):
                if va != 0:
                    avgAgeRatio.append((avgAgeNums[ind + 1] - va)/(va + .0))
                else:
                    if 0 == avgAgeNums[ind + 1]:
                        avgAgeRatio.append(0)
                    else:
                        avgAgeRatio.append(10)

            hosIncreaseRatio = {}
            testData={}
            for eachHos in slideHosData:
                eachHosDays = slideHosData[eachHos]
                eachIncreaseRatio = []
                for ind,va in enumerate(eachHosDays[:-1]):
                    if 0 != va:
                        ratio = (eachHosDays[ind+1] - va)/( va + .0)
                        eachIncreaseRatio.append(ratio)
                    else:
                        if eachHosDays[ind+1] == 0:
                            eachIncreaseRatio.append(0)
                        else:
                            eachIncreaseRatio.append(10)

                hosIncreaseRatio[eachHos] = eachIncreaseRatio
                testData[eachHos] = eachIncreaseRatio[-1]

            dtwDist = {}
            for eachHos in hosIncreaseRatio:
                dtwDist[eachHos] = dis(hosIncreaseRatio[eachHos][:-1],avgAgeRatio[:-1])

            #distanceValue = dtwDist.values()
            #distanceMax = max(distanceValue)
            #distanceMin = min(distanceValue)
            #distanceRange = distanceMax - distanceMin
            dtwDistTmp = copy.deepcopy(dtwDist)

            ##for key,value in dtwDist.iteritems():
            ##    dtwDist[key] = (value - distanceMin)/ (distanceRange + .0)

            #testDataValue = testData.values()
            #testDataMax = max(testDataValue)
            #testDataMin = min(testDataValue)
            #testDataRange = testDataMax - testDataMin

            testDataTmp = copy.deepcopy(testData)
            #for key,value in testData.iteritems():
            #    if (value - testDataMin ==0 )and (testDataRange ==0 ):
            #        testData[key] = 0
            #    else:
            #        testData[key] = (value- testDataMin)/ ( testDataRange + .0)

            weightAll = sorted(dtwDistTmp.iteritems(), key=lambda p: p[1], reverse=False)
            leng = len(weightAll)
            for i in range(leng):
                if weightAll[i][1] < 0.03:
                    hos = weightAll[i][0]
                    differ = testDataTmp[hos] - avgAgeRatio[-1]
                    if differ >= 0.25:
                        j += 1
                        print (start + delta).strftime('%Y-%m-%d'), u"  医院 : ", hos, u"医院名称 ", hosName[hos], u" 医院级别 ", hoslevel[HOSLEVEL], u"该医院历史情况", ','.join(str(x) for x in slideHosData[hos]), u'同级增长率 ', avgAgeRatio[-1], u"该医院本周增长率 ",testDataTmp[hos], u"同级医院本周住院人数 ", avgAgeNums[-1], \
                            u"同级医院上周住院人数 ", avgAgeNums[-2],u'DTW距离',weightAll[i][1],u"该医院当前周人数 ", slideHosData[hos][-1], u"该医院前一周人数 ", slideHosData[hos][-2], u'平均人数 ： ', str(math.ceil(hosSumZydata[hos] / (189.0)))
                        #print u"同级医院增长率", [float('%.3f' % (x)) for x in avgAgeRatio]
                        #print u"#", [float('%.3f' % (x)) for x in hosIncreaseRatio[hos]]

                        fig = pl.figure()
                        ax1 = fig.add_subplot(1, 1, 1)
                        ax1.plot(hosIncreaseRatio[weightAll[i][0]], label=weightAll[i][0])
                        ax1.plot(avgAgeRatio, 'k--', marker='o', label='zt')
                        ax1.set_xlim([0, 26])
                        #ax1.set_ylim([-1, 1])
                        pl.legend()
                        pl.show()
                        pl.close()



    #fp.close()
    print j


if __name__ == "__main__":
    main()