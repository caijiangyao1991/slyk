
# -*- coding: utf-8 -*-

import MySQLdb
from datetime import datetime
from pyExcelerator import *
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import xlrd
import copy

def getRYdata():
    inputfile = "E:/RYjb.csv"
    inputfile1 = "E:/projectmonthjb.csv"
    data = pd.read_csv(inputfile,header=None,names=['month', 'hos', 'jb', 'con'])
    prodata = pd.read_csv(inputfile1, header=None, names=['pro', 'month', 'hos', 'jb', 'con'])
    return data, prodata


def getdata():
    inputfile = "F:/bm.xlsx"
    data = pd.read_excel(inputfile)
    return data

def getmoncost():
    inputfile = "E:\monthavgc.csv"
    inputfile1 = "E:/tcfy.csv"
    data = pd.read_csv(inputfile, header=None,names=['pro', 'hos', 'allcost', 'mons', 'monavg'])
    mondata = pd.read_csv(inputfile1, header=None, names=['pro', 'month', 'hos', 'moncost', 'tcfy'])
    return data, mondata


def zidian(data):
    hv = {}
    for i in data.index:
        hos = data['month'][i]
        cou = data['con'][i]
        hv[hos] = cou
    return hv

def pron(proname):
    pro = {}
    for i in proname.index:
        proid = proname[u'异地项目ID'][i]
        name = proname[u'本地项目名称'][i]
        pro[proid] = name
    return pro
def monc(data,a,b):
    mona={}
    for i in data.index:
        mon = data['month'][i]
        moncost = data['moncost'][i]
        mona[mon] = moncost
    return mona

def monavg(data):
    mondata = {}
    for i in data.index:
        hos = data['hos'][i]
        mona = data['monavg'][i]
        mondata[hos] = mona
    return mondata


def ztRatio(hos,prodata,mondata):
    Ztratio = {}
    ZtHdata = mondata[mondata['hos'] != hos]
    ZtPdata = prodata[prodata['hos'] != hos]
    moncnt = ZtHdata['con'].groupby(ZtHdata['month']).sum()
    promoncet = ZtPdata['con'].groupby(ZtPdata['month']).sum()

    for mon in moncnt.index:
        Ztratio[mon] = promoncet[mon] / (moncnt[mon]+.0)

    return pd.Series(Ztratio)

def Ratioavg(delhos,hos):
    Ratio = {}
    mon = delhos[hos].keys()
    del delhos[hos]
    mon.sort()
    for m in mon:
        lenHos=0
        for h in delhos:
            if delhos[h][m] != None:
                Ratio[m] = Ratio.get(m, 0) + delhos[h][m]
                lenHos += 1
            else:
                Ratio[m] = Ratio.get(m, 0)
        Ratio[m] = Ratio[m]/lenHos
    return pd.Series(Ratio)



def main():
    rydata, prodata = getRYdata()
    avgcost,moncost = getmoncost()
    proname = getdata()
    proidname = pron(proname)

    month = rydata['month'].drop_duplicates()
    pro = prodata['pro'].drop_duplicates()
    for p in pro:
        promonthhos = prodata[prodata['pro'] == p]
        promonavg = avgcost[avgcost['pro'] == p]
        promoncos = moncost[moncost['pro'] == p]
        promon = promonthhos['month'].drop_duplicates()
        if len(promon) + 5 < len(month):
            continue
        prohos = promonthhos['hos'].drop_duplicates()
        mondata = monavg(promonavg)
        hosyearweekfy = {}
        hosmonthcon = {}
        hms = {}
        tcfyhos = {}
        i = 0
        for hos in prohos:
            ratio = {}
            hosdata = promonthhos[promonthhos['hos'] == hos]
            hosmon = promoncos[promoncos['hos'] == hos]
            allhosdata = rydata[rydata['hos'] == hos]
            datatime = hosdata['month']
            prohosmon = datatime.drop_duplicates()
            if len(prohosmon)+5 < len(month):
                continue
            monthcon = zidian(hosdata)
            allmonthcon = zidian(allhosdata)
            moncos = monc(hosmon,'month','moncost')
            hms[hos] = moncos
            tcfyhos[hos] = monc(hosmon,'month','tcfy')

            for m in allmonthcon:
                if m not in monthcon:
                    monthcon[m] = None
                    ratio[m] = None
                    continue
                ratio[m] = monthcon[m]/(allmonthcon[m]+.0)

            for mon in month:
                if mon not in ratio:
                    ratio[mon] = None  #.append([rqjg[j], 0])
            hosyearweekfy[hos] = ratio  # hos:(year,week):人均钱
            hosmonthcon[hos] = allmonthcon
            i += 1

        if p not in proidname:
            proidname[p] = None

        hosstd = {}

        for hos, val in hosyearweekfy.iteritems():
            hosf = pd.Series(val)
            hosstd[hos] = [hosf.std(), hosf.mean()]
            for mon in val:
                if hosyearweekfy[hos][mon] > 3*hosstd[hos][0]+hosstd[hos][1] and hosyearweekfy[hos][mon] >=0.5 and hms[hos][mon] >= 1000:
                    print '项目编码:', p, '项目名称：', proidname[p], hos, \
                         '月份:', mon, '均值:', hosstd[hos][1], '标准差:', hosstd[hos][0], \
                         '当月使用率:', hosyearweekfy[hos][mon], '当月入院人数：',hosmonthcon[hos][mon],\
                         '项目月均费用：', mondata[hos], '当月费用:', hms[hos][mon],'报销金额:',tcfyhos[hos][mon],i


        """
        if p == 'AL1763':
            hos = '什邡市第四人民医院'
            print hosyearweekfy.keys()
            delhos = copy.deepcopy(hosyearweekfy)
            #Ztratio = ztRatio(hos, promonthhos, rydata)
            meanratio = Ratioavg(delhos, hos)
            ra = hosyearweekfy[hos]
            ra1 = pd.Series(ra)
            h = list(meanratio.values)
            #z = list(Ztratio.values)
            y = list(ra1.values)
            x = range(len(ra1))
            group_labels = list(ra1.index)
            plt.plot(x, y, 'r-')
            #plt.plot(x, z, 'b')
            plt.plot(x, h, 'g')
            plt.title('AL1763')
            plt.xticks(x, group_labels, rotation=60)
            plt.grid()
            plt.show()
        """

if __name__ == '__main__':
    main()











