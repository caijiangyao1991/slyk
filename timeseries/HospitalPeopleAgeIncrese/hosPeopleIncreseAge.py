#!/usr/bin/env python
# -*- coding: utf-8 -*-
from tool.DBAcess.postgre import DBAccess
import hashlib
import psycopg2
import xlwt
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as pl
from pandas import DataFrame
from dtw import dtw
from numpy import array
from numpy.linalg import norm
import logging
import copy
import json
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf8')

HISCIRCLE = 27
EACHCIRCLE = 7
age = [1, 2, 3, 4, 5]
HOSLEVEL = ['01', '02', '03']

class logger():
    def __init__(self,path):
        self.path = path

    def __pz(self, path):
        path1 = path + u'\log.log'
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            filename='%s' % path1,
                            filemode='w+')
        # create logger
        logger_name = "age"
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)

        # create file handler
        fh = logging.StreamHandler()
        fh.setLevel(logging.INFO)

        # create formatter
        fmt = "%(asctime)-15s %(levelname)s %(filename)s %(lineno)d  %(message)s"
        datefmt = "%a %d %b %Y %H:%M:%S"
        formatter = logging.Formatter(fmt, datefmt)
        # add handler and formatter to logger
        fh.setFormatter(formatter)
        logging.getLogger('').addHandler(fh)
        return logger

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

def init(cur,rydata,hoses):
    for each in cur:
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
        rydata[hos][yearAndDay[0]][yearAndDay[1]] = int(each[2])
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
    return rydata


def getRydataAge(hoses, dj, nl):
    sqlAge = """
              SELECT t.hosId, t.inHosIime,COUNT(*) AS cnt
               FROM (
             SELECT KC21_AAZ217,extract(YEAR from KC21_CKC537)||'-'||extract(doy from KC21_CKC537)-1 as inHosIime,KB01_CKZ543 as hosId
             FROM t_jsxx_bc
             WHERE KB01_AKA101= '%s' and KC21_JZ_NAME='住院'  and ydjy=0 AND KC21_CKC537 is not null AND KC21_CKC538>=KC21_CKC537
           AND to_char(KC21_CKC537 ,'YYYY-MM') >= '2014-01'  AND   to_char(KC21_CKC537 ,'YYYY-MM') <= '2016-12' and
            (
            CASE
            WHEN skc17_nl <= 6 THEN 1
            WHEN skc17_nl <= 17 AND skc17_nl >= 7 THEN 2
            WHEN skc17_nl <= 40 AND skc17_nl >= 18 THEN 3
            WHEN skc17_nl <= 65 AND skc17_nl >= 41 THEN 4
            WHEN skc17_nl >= 66 THEN 5
            END)=%d) t
              GROUP BY t.hosId,t.inHosIime
        """ % (dj, nl)
    try:
        ryDataAge = {}
        con = psycopg2.connect(host='192.168.10.60', port=5432, user='gpadmin', password='postgres', database='dy')
        cur1 = con.cursor()
        cur1.execute(sqlAge)
        ryDataAge = init(cur1, ryDataAge, hoses)
    except Exception,e:
        print e
        con.close()
        cur1.close()
        return None
    cur1.close()
    con.close()

    return ryDataAge

def getCydataAge(hoses, dj, nl):

    sqlAge = """
                 SELECT T.hosId, T.outHosIime,COUNT (*) AS cnt
            FROM(SELECT KC21_AAZ217,extract(YEAR from KC21_CKC538)||'-'||extract(doy from KC21_CKC538)-1 AS outHosIime,
                KB01_CKZ543 AS hosId FROM t_jsxx_bc
            WHERE KB01_AKA101 = '%s' and KC21_JZ_NAME='住院'and ydjy=0  AND KC21_CKC538 IS NOT NULL AND KC21_CKC538 >= KC21_CKC537 AND to_char(KC21_CKC538,'YYYY-MM') >= '2014-01' AND to_char(KC21_CKC538,'YYYY-MM') <= '2016-12'
            and  (
            CASE
            WHEN skc17_nl <= 6 THEN 1
            WHEN skc17_nl <= 17 AND skc17_nl >= 7 THEN 2
            WHEN skc17_nl <= 40 AND skc17_nl >= 18 THEN 3
            WHEN skc17_nl <= 65 AND skc17_nl >= 41 THEN 4
            WHEN skc17_nl >= 66 THEN 5
            END)=%d   ) T
            GROUP BY T.hosId, T.outHosIime
        """ % (dj,nl)
    try:

        cyDataAge = {}
        con = psycopg2.connect(host='192.168.10.60', port=5432, user='gpadmin', password='postgres', database='dy')

        cur1 = con.cursor()
        cur1.execute(sqlAge)
        cyDataAge = init(cur1, cyDataAge, hoses)
    except Exception,e:
        print e
        cur1.close()
        con.close()
        return None
    con.close()
    cur1.close()
    return cyDataAge


def getRydata(hoses,dj):
    sql ="""
          SELECT t.hosId, t.inHosIime,COUNT(*) AS cnt
           FROM (
         SELECT KC21_AAZ217,extract(YEAR from KC21_CKC537)||'-'||extract(doy from KC21_CKC537)-1 as inHosIime,KB01_CKZ543 as hosId
         FROM t_jsxx_bc
         WHERE KB01_AKA101= '%s' and KC21_JZ_NAME='住院'  and ydjy=0 AND KC21_CKC537 is not null AND KC21_CKC538>=KC21_CKC537
       AND to_char(KC21_CKC537 ,'YYYY-MM') >= '2014-01'  AND   to_char(KC21_CKC537 ,'YYYY-MM') <= '2016-12'
       ) t
          GROUP BY t.hosId,t.inHosIime
    """ % dj

    try:
        rydata = {}
        con = psycopg2.connect(host='192.168.10.60', port=5432, user='gpadmin', password='postgres', database='dy')
        cur = con.cursor()
        cur.execute(sql)
        rydata = init(cur, rydata, hoses)

    except Exception,e:
        print e
        cur.close()
        con.close()
        return None
    cur.close()
    con.close()

    return rydata

def getCydata(hoses, dj):
    sql = """
             SELECT T.hosId, T.outHosIime,COUNT (*) AS cnt
        FROM(SELECT KC21_AAZ217,extract(YEAR from KC21_CKC538)||'-'||extract(doy from KC21_CKC538)-1 AS outHosIime,
            KB01_CKZ543 AS hosId FROM t_jsxx_bc
        WHERE KB01_AKA101 = '%s' and KC21_JZ_NAME='住院'and ydjy=0  AND KC21_CKC538 IS NOT NULL AND KC21_CKC538 >= KC21_CKC537 AND to_char(KC21_CKC538,'YYYY-MM') >= '2014-01' AND to_char(KC21_CKC538,'YYYY-MM') <= '2016-12'\
            ) T
        GROUP BY T.hosId, T.outHosIime
    """ % dj

    try:
        cydata = {}
        con = psycopg2.connect(host='192.168.10.60', port=5432, user='gpadmin', password='postgres', database='dy')
        cur = con.cursor()
        cur.execute(sql)
        cydata = init(cur, cydata, hoses)

    except Exception,e:
        print e
        cur.close()

        con.close()
        return None
    cur.close()
    con.close()


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
    hoses = rydata.keys()
    zydata = initZyData(hoses)
    years = [2014, 2015, 2016]

    for year in years:
        if runnian(year):
            days = range(366)
        else:
            days = range(365)
        for day in days:
            for hos in hoses:
                if (2014 == year and 361 == day):
                    zydata[hos][year][day] = initHos.get(hos, 0) + rydata[hos][year][day]

                else:
                    if (2016 == year and 0 == day) or (2015 == year and 0 == day):
                        if hos not in cydata:
                            zydata[hos][year][day] = zydata[hos][year-1][364] + rydata[hos][year][day]
                        else:
                            zydata[hos][year][day] = zydata[hos][year - 1][364] - cydata[hos][year - 1][364] + \
                                                     rydata[hos][year][day]
                    else:
                        if hos not in cydata:
                            zydata[hos][year][day] = zydata[hos][year][day-1] + rydata[hos][year][day]
                        else:
                            zydata[hos][year][day] = zydata[hos][year][day-1] - cydata[hos][year][day-1] + \
                                                     rydata[hos][year][day]

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

def getInitHosAge(dj,nl):
    zysql ="""
        SELECT tp.hosId,COUNT(*) AS cnt
          FROM (
        SELECT  KC21_AAZ217,KB01_CKZ543 as hosId
          FROM t_jsxx_bc
         WHERE  KB01_AKA101='%s'and KC21_JZ_NAME='住院' and ydjy=0 and KC21_CKC537 is not null and KC21_CKC538>=KC21_CKC537 and to_char(KC21_CKC537,'YYYY-MM-DD') <= '2014-12-27'  AND (to_char(KC21_CKC538,'YYYY-MM-DD') >= '2014-12-27' OR KC21_CKC538 IS NULL)
        and (CASE
            WHEN skc17_nl <= 6 THEN 1
            WHEN skc17_nl <= 17 AND skc17_nl >= 7 THEN 2
            WHEN skc17_nl <= 40 AND skc17_nl >= 18 THEN 3
            WHEN skc17_nl <= 65 AND skc17_nl >= 41 THEN 4
            WHEN skc17_nl >= 66 THEN 5
            END)=%d)tp
          GROUP BY tp.hosId
    """ % (dj, nl)
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
          SELECT ttp.hosId,COUNT (*) AS cnt
          FROM (SELECT KC21_AAZ217, KB01_CKZ543 AS hosId FROM t_jsxx_bc
                WHERE KB01_AKA101 = '%s' AND KC21_JZ_NAME = '住院' and ydjy=0 and	to_char(KC21_CKC538, 'YYYY-MM-DD') = '2014-12-27' AND KC21_CKC538 >= KC21_CKC537 AND
                (CASE
            WHEN skc17_nl <= 6 THEN 1
            WHEN skc17_nl <= 17 AND skc17_nl >= 7 THEN 2
            WHEN skc17_nl <= 40 AND skc17_nl >= 18 THEN 3
            WHEN skc17_nl <= 65 AND skc17_nl >= 41 THEN 4
            WHEN skc17_nl >= 66 THEN 5
            END)=%d) ttp
                GROUP BY ttp.hosId
    """ % (dj, nl)

    con = psycopg2.connect(host='192.168.10.60', port=5432, user='gpadmin', password='postgres', database='dy')
    cur = con.cursor()
    cur.execute(cysql)
    for eachData in cur:
        hosCyData[eachData[0]] = int(eachData[1])

    cur.close()
    con.close()
    initHos = {}
    for hos in hosZyData:
        initHos[hos] = hosZyData[hos] - hosCyData.get(hos, 0)
    return initHos



def getInitHos(dj):
    zysql ="""
        SELECT tp.hosId,COUNT(*) AS cnt
          FROM (
        SELECT  KC21_AAZ217,KB01_CKZ543 as hosId
          FROM t_jsxx_bc
         WHERE  KB01_AKA101='%s'and KC21_JZ_NAME='住院' and ydjy=0 and KC21_CKC537 is not null and KC21_CKC538>=KC21_CKC537 and to_char(KC21_CKC537,'YYYY-MM-DD') <= '2014-12-27'  AND (to_char(KC21_CKC538,'YYYY-MM-DD') >= '2014-12-27' OR KC21_CKC538 IS NULL)
        )tp
          GROUP BY tp.hosId
    """ % dj
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
          SELECT ttp.hosId,COUNT (*) AS cnt
          FROM (SELECT KC21_AAZ217, KB01_CKZ543 AS hosId FROM t_jsxx_bc
                WHERE KB01_AKA101 = '%s' AND KC21_JZ_NAME = '住院' and ydjy=0 and	to_char(KC21_CKC538, 'YYYY-MM-DD') = '2014-12-27' AND KC21_CKC538 >= KC21_CKC537) ttp
                GROUP BY ttp.hosId
    """ % dj

    con = psycopg2.connect(host='192.168.10.60', port=5432, user='gpadmin', password='postgres', database='dy')
    cur = con.cursor()
    cur.execute(cysql)
    for eachData in cur:
        hosCyData[eachData[0]] = int(eachData[1])

    cur.close()
    con.close()
    initHos ={}
    for hos in hosZyData:
        initHos[hos] = hosZyData[hos] - hosCyData.get(hos, 0)
    return initHos





def getBigTenHos(zydata,year,day,hosName):
    checkYearAndDay = {}
    if day < 188:
        begindata = 365-(189-(day+1))
        checkYearAndDay[year-1] = range(begindata,365)
        checkYearAndDay[year] = range(0, day+1)
    else:
        checkYearAndDay[year] = range(day-188, day+1)
    hosBigTen = {}
    for hos in zydata:
        flag = False
        for year in checkYearAndDay:
            for day in checkYearAndDay[year]:
                if zydata[hos][year][day] < 10:
                    flag = True
                    break
        if flag == False:

            hosBigTen[hos]=hosName[hos]


    return hosBigTen

def getHosName(dj):
    sql = """
    SELECT dh.KB01_CKZ543,dh.KB01_CKB519
  FROM d_hospital dh
  WHERE dh.KB01_AKA101='%s'
    """ % dj
    con = psycopg2.connect(host='192.168.10.60', port=5432, user='gpadmin', password='postgres', database='dy')

    cur = con.cursor()
    cur.execute(sql)
    hosName = {}
    for eachData in cur:
        hosName[eachData[0]] = eachData[1]

    cur.close()
    con.close()
    return hosName

def parseYearAndWeek(t):
    t1 = t.split('-')
    t2 = datetime.datetime(int(t1[0]), int(t1[1]), int(t1[2]))
    week = int(t2.strftime("%U"))
    year = int(t2.strftime('%Y'))
    week += 1
    if week == 53:
        year += 1
        week = 1
    if week == 1:
        week = 52
        year -= 1
    else:
        week -= 1
    if week <= 9:
        week = '0' + str(week)
    else:
        week = str(week)
    return '-'.join((str(year), str(week)))

def parseYearAndDay(t):
    t1 = t.split('-')
    t2 = datetime.datetime(int(t1[0]), int(t1[1]), int(t1[2]))
    a = int(t2.strftime('%w')) + 1
    delta = datetime.timedelta(days=a)
    data = t2 - delta
    year = int(data.strftime('%Y'))
    day = int(data.strftime('%j')) - 1
    return year, day
def jsonData(hosIncreaseRatio, avgAgeRatio, yearWeek):
    """
    :param hosIncreaseRatio, avgAgeRatio:
    :return: 返回画图用的data数据
    """
    eachCircleDataYear,eachWeek = yearWeek.split('-')
    eachWeek = int(eachWeek)
    eachCircleDataYear = int(eachCircleDataYear)
    data = []
    weeklen = len(hosIncreaseRatio)
    weekList = range(1, eachWeek + 1)
    interi = 26 - eachWeek
    beginWeek = 26 + eachWeek + 1
    for i in range(weeklen):
        tmp = {}
        if eachWeek >= 25:
            week = '-'.join((str(eachCircleDataYear), str(eachWeek - 25 + i)))
            curHosIncRate = hosIncreaseRatio[i]
            baselevelIncRate = avgAgeRatio[i]
            tmp['week'] = week
            tmp['curHosGrowthRate'] = str(curHosIncRate)
            tmp['baseLevGrowthRate'] = str(baselevelIncRate)
            data.append(tmp)
        else:
            if i < interi:
                week = '-'.join((str(eachCircleDataYear - 1), str(beginWeek + i)))
                curHosIncRate = hosIncreaseRatio[i]
                baselevelIncRate = avgAgeRatio[i]
                tmp['week'] = week
                tmp['curHosGrowthRate'] = str(curHosIncRate)
                tmp['baseLevGrowthRate'] = str(baselevelIncRate)
                data.append(tmp)
            else:
                week = '-'.join((str(eachCircleDataYear), str(weekList[i - interi])))
                curHosIncRate = hosIncreaseRatio[i]
                baselevelIncRate = avgAgeRatio[i]
                tmp['week'] = week
                tmp['curHosGrowthRate'] = str(curHosIncRate)
                tmp['baseLevGrowthRate'] = str(baselevelIncRate)
                data.append(tmp)

    return data

def allIncrese(slideHosData,slideHosDataAge):
    allDataFrame = DataFrame(slideHosData.values())
    allData = allDataFrame.sum(axis=0)
    avgNums = list(allData.values)

    allDataFrameAge = DataFrame(slideHosDataAge.values())
    allDataAge = allDataFrameAge.sum(axis=0)
    avgAgeNums = list(allDataAge.values)
    AgeRatio = []
    for ind, va in enumerate(avgAgeNums):
        if avgNums[ind] != 0:
            AgeRatio.append(va / (avgNums[ind] + .0))
        else:
            if 0 == avgNums[ind]:
                AgeRatio.append(0)

    return AgeRatio

def hosIncrese(slideHosData,slideHosDataAge):
    hosIncreaseRatio = {}
    testData = {}
    for eachHos in slideHosDataAge:
        eachHosDaysAge = slideHosDataAge[eachHos]
        eachHosDays = slideHosData[eachHos]
        eachIncreaseRatioAge = []
        for ind, va in enumerate(eachHosDaysAge):
            if 0 != eachHosDays[ind]:
                ratio = va / (eachHosDays[ind] + .0)
                eachIncreaseRatioAge.append(ratio)
            else:
                eachIncreaseRatioAge.append(0)
        hosIncreaseRatio[eachHos] = eachIncreaseRatioAge
        testData[eachHos] = eachIncreaseRatioAge[-1]
    return hosIncreaseRatio,testData

def PropAgeRatio(hosIncreasePropAge, avgAgePropAge):
    hosIncreaseRatioAge = {}
    for hos in hosIncreasePropAge:
        increseratio = []
        for ind, va in enumerate(hosIncreasePropAge[hos][:-1]):
            increseratio.append((hosIncreasePropAge[hos][ind + 1]-va)/(va + .0))
        hosIncreaseRatioAge[hos] = increseratio
    avgAgeRatioAge = []
    for ind, va in enumerate(avgAgePropAge[:-1]):
        avgAgeRatioAge.append((avgAgePropAge[ind+1]-va)/(va+.0))

    return hosIncreaseRatioAge,avgAgeRatioAge

def weekrange(year,day):
    endY = datetime.datetime(year, 1, 1)
    delend = datetime.timedelta(days=day)
    endTime = (endY + delend).strftime('%Y-%m-%d')

    if day < 6:
        startDay = 365-(7-(day+1))
        year -= 1
        startY = datetime.datetime(year, 1, 1)
        delta = datetime.timedelta(days=startDay)
        startTime = (startY + delta).strftime('%Y-%m-%d')
    else:
        startDay = day - 6
        startY = datetime.datetime(year, 1, 1)
        delta = datetime.timedelta(days=startDay)
        startTime = (startY + delta).strftime('%Y-%m-%d')
    return startTime, endTime

def increse_Reason(startTime,endTime,startLastTime,endLastTime,hosid,nl,week):
    db = DBAccess('dy')
    db.get_con()

    # TODO 提取本周总人数
    curWeekSql = """select count(*) from t_jsxx_bc where to_char(kc21_ckc537,'YYYY-MM-DD')<='%s' and to_char(kc21_ckc538,'YYYY-MM-DD')>='%s' and
      kc21_jz_name='住院' and ydjy=0 and KB01_CKZ543='%s'""" % (endTime, startTime, hosid)
    curWeekTotalpeople = db.get_db_df(curWeekSql)
    curWeekTotalpeople = int(curWeekTotalpeople['count'][0])
    print curWeekTotalpeople

    #TODO 提取本周该年龄段各科室人数
    curWeekKs = """select KC21_CKC543,count(*) from t_jsxx_bc where to_char(kc21_ckc537,'YYYY-MM-DD')<='%s' and to_char(kc21_ckc538,'YYYY-MM-DD')>='%s' and
          kc21_jz_name='住院' and ydjy=0 and KB01_CKZ543='%s' and
           (CASE
            WHEN skc17_nl <= 6 THEN 1
            WHEN skc17_nl <= 17 AND skc17_nl >= 7 THEN 2
            WHEN skc17_nl <= 40 AND skc17_nl >= 18 THEN 3
            WHEN skc17_nl <= 65 AND skc17_nl >= 41 THEN 4
            WHEN skc17_nl >= 66 THEN 5
            END)=%d group by KC21_CKC543""" % (endTime, startTime, hosid, nl)
    curWeekKspeople = db.get_db_df(curWeekKs)

    curWeekKspeople['rata'] = curWeekKspeople['count'].apply(lambda x: x/(curWeekTotalpeople+.0))

    #TODO 提取本周该年龄段各疾病人数
    curWeekJb = """select substr(KC21_AKC196,1,3) as KC21_AKC196,count(*) from t_jsxx_bc where to_char(kc21_ckc537,'YYYY-MM-DD')<='%s' and to_char(kc21_ckc538,'YYYY-MM-DD')>='%s' and
              kc21_jz_name='住院' and ydjy=0 and KB01_CKZ543='%s' and
               (CASE
                WHEN skc17_nl <= 6 THEN 1
                WHEN skc17_nl <= 17 AND skc17_nl >= 7 THEN 2
                WHEN skc17_nl <= 40 AND skc17_nl >= 18 THEN 3
                WHEN skc17_nl <= 65 AND skc17_nl >= 41 THEN 4
                WHEN skc17_nl >= 66 THEN 5
                END)=%d group by substr(KC21_AKC196,1,3)""" % (endTime, startTime, hosid, nl)
    curWeekJbpeople = db.get_db_df(curWeekJb)
    curWeekJbpeople['rata'] = curWeekJbpeople['count'].apply(lambda x: x/(curWeekTotalpeople+.0))





    # TODO 提取上周总人数
    lastWeekSql = """select count(*) from t_jsxx_bc where to_char(kc21_ckc537,'YYYY-MM-DD')<='%s' and to_char(kc21_ckc538,'YYYY-MM-DD')>='%s' and
          kc21_jz_name='住院' and ydjy=0 and KB01_CKZ543='%s'""" % (endLastTime,startLastTime,hosid)
    lastWeekTotalpeople = db.get_db_df(lastWeekSql)
    lastWeekTotalpeople = int(lastWeekTotalpeople['count'][0])
    print lastWeekTotalpeople

    #TODO 提取上周该年龄段各科室人数
    lastWeekKs = """select KC21_CKC543,count(*) from t_jsxx_bc where to_char(kc21_ckc537,'YYYY-MM-DD')<='%s' and to_char(kc21_ckc538,'YYYY-MM-DD')>='%s' and
          kc21_jz_name='住院' and ydjy=0 and KB01_CKZ543='%s' and
           (CASE
            WHEN skc17_nl <= 6 THEN 1
            WHEN skc17_nl <= 17 AND skc17_nl >= 7 THEN 2
            WHEN skc17_nl <= 40 AND skc17_nl >= 18 THEN 3
            WHEN skc17_nl <= 65 AND skc17_nl >= 41 THEN 4
            WHEN skc17_nl >= 66 THEN 5
            END)=%d group by KC21_CKC543""" % (endLastTime, startLastTime, hosid, nl)
    lastWeekKspeople = db.get_db_df(lastWeekKs)
    lastWeekKspeople['rata'] = lastWeekKspeople['count'].apply(lambda x: x/(lastWeekTotalpeople+.0))

    # TODO 提取上周该年龄段各疾病人数
    lastWeekJb = """select substr(KC21_AKC196,1,3) as KC21_AKC196,count(*) from t_jsxx_bc where to_char(kc21_ckc537,'YYYY-MM-DD')<='%s' and to_char(kc21_ckc538,'YYYY-MM-DD')>='%s' and
              kc21_jz_name='住院' and ydjy=0 and KB01_CKZ543='%s' and
               (CASE
                WHEN skc17_nl <= 6 THEN 1
                WHEN skc17_nl <= 17 AND skc17_nl >= 7 THEN 2
                WHEN skc17_nl <= 40 AND skc17_nl >= 18 THEN 3
                WHEN skc17_nl <= 65 AND skc17_nl >= 41 THEN 4
                WHEN skc17_nl >= 66 THEN 5
                END)=%d group by substr(KC21_AKC196,1,3)""" % (endLastTime, startLastTime, hosid, nl)
    lastWeekJbpeople = db.get_db_df(lastWeekJb)
    lastWeekJbpeople['rata'] = lastWeekJbpeople['count'].apply(lambda x: x / (lastWeekTotalpeople + .0))
    # TODO 疾病分析结果
    JbRata = pd.merge(curWeekJbpeople, lastWeekJbpeople, on='kc21_akc196',how='left')
    JbRata.rename(columns={'rata_x': u'本周占比', 'rata_y': u'上周占比', 'kc21_akc196':u'疾病', 'count_x':u'本周人数', 'count_y': u'上周人数'},inplace=True)
    JbRata.sort_values(by=[u'本周占比'], inplace=True, ascending=False)
    JbRata.to_excel(u'F:\年龄分布异常\%s疾病%s.xlsx' % (hosid,week))
    # TODO 科室分析结果
    ksRata = pd.merge(curWeekKspeople,lastWeekKspeople, on='kc21_ckc543', how='left')
    ksRata.rename(columns={'rata_x': u'本周占比', 'rata_y': u'上周占比', 'kc21_ckc543':u'科室', 'count_x':u'本周人数', 'count_y':u'上周人数'},inplace=True)
    ksRata.sort_values(by=[u'本周占比'], inplace=True, ascending=False)
    ksRata.to_excel(u'F:\年龄分布异常\%s科室%s.xlsx' % (hosid,week))


def main():

    hoslevel = {'01': u"一级医院", '02': u"二级医院", '03': u"三级医院"}
    ageName = {1: u'<= 6', 2: u'7-17', 3: u'18-40',4: u'41-65', 5: u'>= 66'}
    w = xlwt.Workbook(encoding='utf-8')
    ws = w.add_sheet('sheet1')
    outpile = u'F:\年龄分布异常\新\Exresule.csv'

    j = 1
    for dj in HOSLEVEL:
        hosName = getHosName(dj)
        initHos = getInitHos(dj)
        rydata = getRydata(initHos, dj)
        cydata = getCydata(initHos, dj)
        zydata = getZyData(rydata, cydata, initHos)
        for nl in age:
            initHosAge = getInitHosAge(dj, nl)
            rydataAge = getRydataAge(initHos, dj, nl)
            cydataAge = getCydataAge(initHos, dj, nl)
            zydataAge = getZyData(rydataAge, cydataAge, initHosAge)


            circleYearsAndDays = {2015: range(185, 365, 7), 2016: range(3, 181, 7)}
            years = [2015, 2016]

            for year in years:
                #print year
                start = datetime.datetime(year, 1, 1)
                for day in circleYearsAndDays[year]:
                    delta = datetime.timedelta(days=day)
                    t = (start + delta).strftime('%Y-%m-%d')
                    (year, day) = parseYearAndDay(t)
                    startTime, endTime = weekrange(year, day)
                    hosBigTenAge = getBigTenHos(zydataAge, year, day, hosName)
                    slideHosData = {}
                    hosSumZydata = {}

                    slideHosDataAge = {}
                    hosSumZydataAge = {}
                    for hos in hosBigTenAge:
                        tmphosAge ,sumZyDataAge = getCircleData(rydataAge, zydataAge, hos, year, day)
                        slideHosDataAge[hos] = tmphosAge
                        hosSumZydataAge[hos] = sumZyDataAge

                        tmphos, sumZyData = getCircleData(rydata, zydata, hos, year, day)
                        slideHosData[hos] = tmphos
                        hosSumZydata[hos] = sumZyData
                    HisAvgProp={}
                    for hos in slideHosDataAge:
                        ageSumData = sum(slideHosDataAge[hos][:26])
                        sumData = sum(slideHosData[hos][:26])
                        HisAvgProp[hos] = ageSumData/(sumData+.0)

                    avgAgePropAge = allIncrese(slideHosData,slideHosDataAge)
                    hosIncreasePropAge, testDataAge = hosIncrese(slideHosData,slideHosDataAge)
                    hosIncreaseRatioAge, avgAgeRatioAge = PropAgeRatio(hosIncreasePropAge, avgAgePropAge)

                    dtwDist = {}
                    for eachHos in hosIncreaseRatioAge:
                        dtwDist[eachHos] = dis(hosIncreaseRatioAge[eachHos][:-1], avgAgeRatioAge[:-1])
                    for hos in dtwDist:
                        if dtwDist[hos] <= 0.03:
                            #hos = weightAll[i][0]
                            differ = hosIncreaseRatioAge[hos][-1] - avgAgeRatioAge[-1]
                            if differ >= 0.3:
                                jsonMap = {"type": u"住院病人年龄分布异常", "common": None, "tabs": []}
                                tabsMap = {"echartsType": "bar", "echartsUnit": "%", "name": u"年龄段增长率趋势", "data": []}
                                yearWeek = parseYearAndWeek(t)
                                # print (start + delta).strftime('%Y-%m-%d'), startTime, endTime,yearWeek, u"  医院 : ", hos, u"医院名称 ", hosName[hos], u" 医院级别 ", hoslevel[dj], \
                                #     u'异常年龄段', ageName[nl],u'当周该年龄段在院人数',slideHosDataAge[hos][-1],u'历史平均占比',HisAvgProp[hos],\
                                #     u'当周该年龄段占比',testDataAge[hos],u"上周该年龄段占比 ", hosIncreasePropAge[hos][-2], u"该年龄段当周占比增长率 ", hosIncreaseRatioAge[hos][-1], \
                                #     u'同级医院当周该年龄段占比', avgAgePropAge[-1], u"同级医院上周该年龄段占比 ", avgAgePropAge[-2], u"同级医院该年龄段当周占比增长率 ", avgAgeRatioAge[-1], \
                                #     u'DTW距离',dtwDist[hos],slideHosData[hos][-1]-slideHosData[hos][-2],slideHosDataAge[hos][-1]-slideHosDataAge[hos][-2]

                                commonParar = {}
                                commonParar['anomalyWeek'] = yearWeek
                                commonParar['hospitalName'] = hosName[hos]
                                commonParar['hosLevel'] = hoslevel[dj]

                                commonParar['ageRange'] = ageName[nl]
                                commonParar['ageRangeCount'] = slideHosDataAge[hos][-1]
                                commonParar['ageRageAvgRate'] = HisAvgProp[hos]

                                commonParar['ageRangeRate'] = str(testDataAge[hos])
                                #commonParar['lastWeekAgeProp'] = str(hosIncreasePropAge[hos][-2])
                                commonParar['curWeekAgePropGrowthRate'] = str(hosIncreaseRatioAge[hos][-1])

                                #commonParar['sameLevCurWeekAgeProp'] = str(avgAgePropAge[-1])
                                #commonParar['sameLevLastWeekAgeProp'] = str(avgAgePropAge[-2])
                                commonParar['sameLevCurWeekAgeGrowthRate'] = str(avgAgeRatioAge[-1])
                                data = jsonData(hosIncreaseRatioAge[hos], avgAgeRatioAge, yearWeek)
                                tabsMap["data"] = data
                                jsonMap["tabs"].append(tabsMap)
                                jsonMap["common"] = commonParar
                                finalResult = json.dumps(jsonMap, ensure_ascii=False)
                                outputfile = u'F:\年龄分布异常\新\Json%s%s.txt' % (yearWeek, hos)
                                f = open(outputfile, 'w')
                                f.write(finalResult)
                                f.close()
                                hospital = hosName[hos]
                                hospital = hospital.decode('utf-8')
                                m2 = hashlib.md5()
                                m2.update(finalResult)
                                md5 = m2.hexdigest()
                                print md5
                                hosLevelInt = int(dj)
                                ws.write(j, 0, u'住院人数增长异常')
                                ws.write(j, 1, 24)
                                ws.write(j, 2, 1)
                                ws.write(j, 3, u'医院')
                                ws.write(j, 6, '20170508')
                                #ws.write(i, 7, zfy)
                                #ws.write(i, 8, bxfy)
                                ws.write(j, 9, 1)
                                ws.write(j, 10, u'未调查')
                                ws.write(j, 12, hos)
                                ws.write(j, 13, hospital)
                                ws.write(j, 17, 2)
                                ws.write(j, 18, hosLevelInt)
                                ws.write(j, 21, finalResult)
                                ws.write(j, 22, md5)
                                ws.write(j, 23, 0)
                                j += 1

                                fig = pl.figure()
                                ax1 = fig.add_subplot(1, 1, 1)
                                ax1.plot(hosIncreaseRatioAge[hos], label=hos)
                                ax1.plot(avgAgeRatioAge, 'k--', marker='o', label='zt')
                                ax1.set_xlim([0, 26])
                                ax1.set_ylim([-1, 1])
                                pl.legend()
                                pl.savefig('F:\python\hospitalPeople\pic\\ni.png')
                                pl.show()
                                pl.close()

    w.save(outpile)
if __name__ == "__main__":
    main()
    # increse_Reason('2016-06-19', '2016-06-25', '2016-06-12', '2016-06-18', 'F0041', 5, '2016-26')
    # increse_Reason('2015-07-26', '2015-08-01', '2015-07-19', '2015-07-25', 'F0101', 4, '2015-31')
    # increse_Reason('2015-10-04', '2015-10-10', '2015-09-27', '2015-10-03', 'F0016', 1, '2015-41')