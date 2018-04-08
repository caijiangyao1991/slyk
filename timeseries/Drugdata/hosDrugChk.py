# -*- coding: utf-8 -*-


from datetime import datetime
from pyExcelerator import *
import numpy as np
import pandas as pd
import xlrd
import hashlib
import json
import sys
reload(sys)
sys.setdefaultencoding('utf8')


def parseYearAndWeek(t):
    """
    :param t:入院（出院）日期 String
    :return:入院（出院）的年份 和 是该年的第几周
    """

    t1 = t.split('/')
    t2 = datetime(int(t1[0]), int(t1[1]), int(t1[2]))
    week = int(t2.strftime("%U"))
    week += 1
    if week <= 9:
        wek = '0' + str(week)
    else:
        wek = str(week)
    year = int(t1[0])
    if wek == '53':
        wek = '01'
        year += 1

    return ''.join((str(year), wek))

def getmedian(hosdata):
    weekdata = {}
    hosmedian = {}
    for i in hosdata.index:
        wek = hosdata['a1'][i]
        cos = hosdata['a5'][i]
        if wek in weekdata:
            weekdata[wek].append(cos)
        else:
            weekdata[wek] = [cos]
    for week,var in weekdata.iteritems():
        vardedian = pd.Series(var).median()
        hosmedian[week] = vardedian
    return hosmedian

def hosavg(yiyuan,hosyydata1,rqjg):
    hosyearweekfy = {}
    hosyearweekcs = {}
    hosypsum = {}
    hosweekbxfy = {}
    hosweekcost = {}
    for hos in yiyuan:
        hosdata = hosyydata1.ix[yiyuan[hos], :]
        datatime = hosdata['a1']
        cost = hosdata['a5']
        hosfy = cost.groupby(datatime).sum()  # 周 sum(钱)
        ypbxfy = hosdata['a7'].groupby(hosdata['a1']).sum()
        yccost = hosdata['a6'].groupby(hosdata['a1']).sum()
        hosrqjg = list(hosfy.index)
        if len(hosrqjg) + 10 < len(rqjg):
            continue

        data = hosdata.ix[:, ['a0', 'a1']].drop_duplicates()
        hoscs = {}  # 周：人数
        for i in list(data.index):
            week = data['a1'][i]
            hoscs[week] = hoscs[week] + 1 if week in hoscs else 1
        for i in list(hosfy.index):
            hosfy[i] = hosfy[i] / (hoscs[i] + .0)

        for j in range(len(rqjg)):
            if rqjg[j] not in hosrqjg:
                hosfy[rqjg[j]] = 0  # .append([rqjg[j], 0])
                hoscs[rqjg[j]] = 0  # .append([rqjg[j], 0])
        hosyearweekfy[hos] = hosfy  # hos:(year,week):人均钱
        hosyearweekcs[hos] = hoscs  # hos:(year,week):人数
        hosypsum[hos] = hosdata['a6'].sum()
        hosweekbxfy[hos] = ypbxfy
        hosweekcost[hos] = yccost
    return hosyearweekfy, hosyearweekcs, hosypsum, hosweekbxfy, hosweekcost

def hosmedian(yiyuan,hosyydata1,rqjg):
    hosyearweekfy={}
    hosyearweekcs={}
    hosypsum={}
    hosweekbxfy = {}
    hosweekcost = {}
    for hos in yiyuan:
        hosdata = hosyydata1.ix[yiyuan[hos], :]
        hosfy = getmedian(hosdata)
        ypbxfy = hosdata['a7'].groupby(hosdata['a1']).sum()
        yccost = hosdata['a6'].groupby(hosdata['a1']).sum()
        hosrqjg = hosfy.keys()
        if len(hosrqjg) + 10 < len(rqjg):
            continue
        data = hosdata.ix[:, ['a0', 'a1']].drop_duplicates()
        hoscs = {}  # 周：人数
        for i in list(data.index):
            week = data['a1'][i]
            hoscs[week] = hoscs[week] + 1 if week in hoscs else 1
        for j in range(len(rqjg)):
            week = rqjg[j]
            if week not in hosrqjg:
                hosfy[week] = 0  # .append([rqjg[j], 0])
                hoscs[week] = 0  # .append([rqjg[j], 0])
        hosyearweekfy[hos] = hosfy  # hos:(year,week):人均钱
        hosyearweekcs[hos] = hoscs  # hos:(year,week):人数
        hosypsum[hos] = hosdata['a6'].sum()
        hosweekbxfy[hos] = ypbxfy
        hosweekcost[hos] = yccost
    return hosyearweekfy, hosyearweekcs, hosypsum, hosweekbxfy, hosweekcost

def analyse(datayp,rqjg,yydata,ypfzname):
    Resulta = []
    Resultm = []
    costavg = {}
    costmed = {}
    for keys in yydata:
        hosyydata1 = datayp.ix[yydata[keys],:]
        ypfzjg1 = hosyydata1['a6'].groupby(hosyydata1['a1']).sum() # [year.week,值]

        hosrqjg1 = list(ypfzjg1.index)
        if len(hosrqjg1) + 10 < len(rqjg):
            continue
        yiyuan = hosyydata1.index.groupby(hosyydata1['a4'])
        hosyweekavgfy, hosyweekavgcs, hosavgsum,hosweekbxfy,hosweekcost = hosavg(yiyuan, hosyydata1, rqjg)
        hosyearweekfy, hosyearweekcs, hosypsum,hosweekbxfy,hosweekcost = hosmedian(yiyuan, hosyydata1, rqjg)
        costavg[keys] = hosyweekavgfy
        costmed[keys] = hosyearweekfy

        hosstd = {}
        for hos in hosyweekavgfy:
            hosstd[hos]= [hosyweekavgfy[hos].std() , hosyweekavgfy[hos].mean()]
            for week in list(hosyweekavgfy[hos].index):
                if hosyweekavgfy[hos][week] > 3*hosstd[hos][0]+hosstd[hos][1] and hosyweekavgcs[hos][week]>2:
                    Resulta.append([keys, ypfzname[keys], ypfzjg1.sum(), hosweekbxfy[hos][week], hosweekcost[hos][week], hos, hosavgsum[hos], hosstd[hos][1], hosstd[hos][0], week, \
                        hosyweekavgfy[hos][week], hosyweekavgcs[hos][week]])

        stdme = {}
        for hos, val in hosyearweekfy.iteritems():
            hosf = pd.Series(val)
            stdme[hos] = [hosf.std(), hosf.mean()]
            for week in val:
                if hosyearweekfy[hos][week] > 3*stdme[hos][0]+stdme[hos][1] and hosyearweekcs[hos][week] > 2:
                    Resultm.append([keys, ypfzname[keys], ypfzjg1.sum(), hosweekbxfy[hos][week], hosweekcost[hos][week], hos, hosypsum[hos], stdme[hos][1], stdme[hos][0],week,\
                    hosyearweekfy[hos][week], hosyearweekcs[hos][week]])

    columns_name = ['ypid', 'yp_name', 'ypcost', 'ypbx', 'ypwfy', 'hos', 'yphoscost','avg','std', 'sj','nowc','cnt']
    Resultavg = pd.DataFrame(Resulta, columns=columns_name)
    Resultmed = pd.DataFrame(Resultm, columns=columns_name)
    Result = pd.merge(Resultavg, Resultmed, left_on=['ypid', 'hos', 'sj'], right_on=['ypid', 'hos', 'sj'])
    return Result, costavg, costmed


def set123(qsavg,qsmed):
    data = []
    week = qsmed.keys()
    week.sort()
    for wek in week:
        tableMap = {}
        tableMap['sj'] = wek
        tableMap['costmed'] = qsmed[wek]
        tableMap['costavg'] = qsavg[wek]
        data.append(tableMap)

    return data

def main():
    inputfile = "D:/t.csv"
    datayp1 = pd.read_csv(inputfile, header=None, names=['a0', 'a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7'])
    print datayp1.head()
    ypfzname={}
    #提取药品组名称
    datayp2=datayp1.ix[:, ['a2', 'a3']].drop_duplicates()
    for i in list(datayp2.index):
        ypfzname[datayp2['a2'][i]]=datayp2['a3'][i] if datayp2['a2'][i] in ypfzname else datayp2['a3'][i]
    totleyydata = np.array(datayp1)
    for i in range(len(totleyydata)):
        totleyydata[i][1] = parseYearAndWeek(totleyydata[i][1])
    datayp = pd.DataFrame(totleyydata, columns=['a0','a1','a2','a3','a4','a5', 'a6', 'a7'])
    jg = datayp['a5'].groupby(datayp['a1']).sum() # week 钱
    yydata = datayp.index.groupby(datayp['a2']) #{药品组：index}
    rqjg = list(jg.index)

    Result, costavg, costmed = analyse(datayp, rqjg, yydata, ypfzname)
    Result = Result.rename(columns={'yp_name_x':'yp_name','ypbx_x':'ypbx', 'ypwfy_x':'ypwfy', 'ypcost_x':'ypcost','yphoscost_x':'yphoscost',\
                                    'avg_x': 'avg','std_x':'std','nowc_x':'nowavg','nowc_y':'nowmedian','cnt_x':'cnt'})
    Result.drop(['yp_name_y','ypbx_y','ypwfy_y', 'ypcost_y', 'yphoscost_y', 'avg_y', 'std_y', 'cnt_y'], axis='columns', inplace=True)

    outfile = "F:/ExResult1.xls"
    w = Workbook()  # 创建一个工作簿
    ws = w.add_sheet('sheet')  # 创建一个工作表
    for i in Result.index:
        sj, ypid, name = str(Result['sj'][i]), Result['ypid'][i], Result['hos'][i]
        if '人民' in name:
            continue
        jsonMap = {"type": u"医院药品使用异常波动", "common": None, "tabs": []}
        ratioFigureMap = {"echartsType": "bar", "name": u"异常趋势图", "data": []}
        commonParar = {}
        commonParar['drugName'] = str(Result['yp_name'][i])
        commonParar['drugCost'] = str(Result['ypcost'][i])
        commonParar['drugzfy'] = str(Result['ypwfy'][i])
        commonParar['drugbxft'] = str(Result['ypbx'][i])
        commonParar['drugHosCost'] = str(Result['yphoscost'][i])
        commonParar['hos'] = str(Result['hos'][i])
        commonParar['avg_cost'] = str(Result['avg'][i])
        commonParar['std_cost'] = str(Result['std'][i])
        commonParar['costavg'] = str(Result['nowavg'][i])
        commonParar['costmed'] = str(Result['nowmedian'][i])
        commonParar['sj'] = str(Result['sj'][i])
        commonParar['cnt'] = str(Result['cnt'][i])
        qsavg, qsmed = costavg[ypid][name], costmed[ypid][name]
        data = set123(qsavg, qsmed)
        ratioFigureMap["data"] = data
        jsonMap["common"] = commonParar
        jsonMap["tabs"].append(ratioFigureMap)
        finalResult = json.dumps(jsonMap, ensure_ascii=False)
        outputfile = "F:/result2/Json%s%s.txt" % (sj, ypid)
        f = open(outputfile, 'w')
        f.write(finalResult)
        f.close()

        m2 = hashlib.md5()
        m2.update(finalResult)
        md5 = m2.hexdigest()
        zfy, bxfy = str(Result['ypwfy'][i]), str(Result['ypbx'][i])


        hos = u'医院'
        name = name.decode('utf-8')
        ws.write(i, 2, 1)
        ws.write(i, 3, hos)
        ws.write(i, 6, sj)
        ws.write(i, 7, zfy)
        ws.write(i, 8, bxfy )
        ws.write(i, 9, 1)
        ws.write(i, 10, u'未调查')
        ws.write(i, 12, name)
        ws.write(i, 17, 2)
        ws.write(i, 21, finalResult)
        ws.write(i, 22, md5)
        ws.write(i, 23, 0)
    w.save(outfile)

if __name__ == '__main__':
    main()
