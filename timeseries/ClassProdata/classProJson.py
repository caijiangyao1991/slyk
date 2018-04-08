
# -*- coding: utf-8 -*-


from datetime import datetime
from pyExcelerator import *
import pandas as pd
import json
import hashlib
import sys
reload(sys)
sys.setdefaultencoding('utf8')


def parseYearAndWeek(t):
    """
    :param t:入院（出院）日期 String
    :return:入院（出院）的年份 和 是该年的第几周
    """

    t1 = t.split('/')
    t2 = datetime(int(t1[0]),int(t1[1]),int(t1[2]))
    week = int(t2.strftime("%U"))
    year = int(t1[0])

    return ''.join((str(year),str(week)))


def getmedian(hosdata):
    weekdata = {}
    hosmedian = {}
    for i in hosdata.index:
        wek = hosdata['a3'][i]
        cos = hosdata['a7'][i]
        if wek in weekdata:
            weekdata[wek].append(cos)
        else:
            weekdata[wek] = [cos]
    for week,var in weekdata.iteritems():
        vardedian = pd.Series(var).median()
        hosmedian[week] = vardedian
    return hosmedian

def zidian(da):
    zidian = {}
    zidian6 = {}
    a = da[u'本地项目编码']
    b = da[u'本地项目名称']
    c = da[u'本地项目数字六位']
    for i in da.index:
        zidian[a[i]] = b[i]
        zidian6[a[i]] = c[i]
    return zidian,zidian6


def main():
    inputfile = "E:\Desktop\ProweekGroup.csv"
    inputfile1 = 'F:/logcResult1.xlsx'
    dataproject1 = pd.read_csv(inputfile, header=None, names=['a1', 'a2', 'a3', 'a4','a5','a6','a7','a8'])
    dataproject1 = dataproject1[dataproject1['a3'] > 201352]
    dataproject1 = dataproject1[dataproject1['a4'].str.contains(r'.*?人民*')==False]
    dataname = pd.read_excel(inputfile1)
    print dataproject1.head()
    print dataname.head()
    dataproject = pd.merge(dataproject1, dataname, left_on='a2', right_on=u'异地项目ID',\
                           sort=False, copy=False)

    print dataproject.head()

    allcost = dataproject['a6'].groupby(dataproject['a3']).sum() # week 钱
    xmdata = dataproject.index.groupby(dataproject[u'本地项目编码']) #{项目：index}
    rqjg = list(allcost.index)

    da = dataproject.ix[:, [u'本地项目编码', u'本地项目数字六位', u'本地项目名称']].drop_duplicates()

    xmname,xmnum6 = zidian(da)
    resulta = []
    resultm = []
    cladata = {}
    for keys in xmdata:
        var = xmdata[keys]
        hosxmdata = dataproject.ix[var,:]
        projweekcost = hosxmdata['a6'].groupby(hosxmdata['a3']).sum() # [year.week,值]
        if len(projweekcost) + 10 < len(allcost):
            continue
        yiyuan = hosxmdata.index.groupby(hosxmdata['a4'])
        hosyearweekfy = {}
        hosyearweekcs = {}
        hosypsum={}
        hosweekavg= {}
        hoszfy = {}
        hosbxfy = {}
        for hos in yiyuan:
            vaind = yiyuan[hos]
            hosdata = hosxmdata.ix[vaind, :]
            datatime = hosdata['a3']
            zfy = hosdata['a6']
            bxfy = hosdata['a8']
            hosazfy = zfy.groupby(datatime).sum()  # 周 sum(钱)
            hosabxfy = bxfy.groupby(datatime).sum()  # 周 sum(钱)
            hosavgcos = hosdata['a7'].groupby(datatime).sum()
            hosmedian = getmedian(hosdata)
            hosrqjg = hosmedian.keys()

            if len(hosrqjg)+10 < len(rqjg):
                continue

            data = hosdata.ix[:, ['a1', 'a3']].drop_duplicates()
            hoscs = {}  # 周：人数
            for i in list(data.index):
                week = data['a3'][i]
                hoscs[week] = hoscs[week] + 1 if week in hoscs else 1
            for i in hosrqjg:
                hosavgcos[i] = hosavgcos[i] / (hoscs[i] + .0)
            for j in range(len(rqjg)):
                week = rqjg[j]
                if week not in hosrqjg:
                    #hosallcos[week] = 0  #.append([rqjg[j], 0])
                    hosavgcos[week] = 0
                    hosmedian[week] = 0
                    hoscs[week] = 0  #.append([rqjg[j], 0])
            hosyearweekfy[hos] = hosmedian  # hos:(year,week):人均钱
            hosweekavg[hos] = hosavgcos
            hosyearweekcs[hos] = hoscs  # hos:(year,week):人数
            hosypsum[hos] = hosdata['a6'].sum()
            hoszfy[hos] = hosazfy
            hosbxfy[hos] = hosabxfy
        hosstd={}
        for hos,val in hosyearweekfy.iteritems():
            hosf = pd.Series(val)
            hosstd[hos] = [hosf.std(),hosf.mean()]
            for week in val:
                if hosyearweekfy[hos][week]>3*hosstd[hos][0]+hosstd[hos][1] and hosyearweekcs[hos][week] > 2:
                    print '项目编码:', keys,'项目名称：',xmname[keys],'6位数编码：',xmnum6[keys], '项目总费用:', projweekcost.sum() \
                        , hos, '医院使用项目的总费用:', hosypsum[hos], \
                        '均值:', hosstd[hos][1], '标准差:', hosstd[hos][0], '周:', week, \
                        '当周中位数:', hosyearweekfy[hos][week], '当周入院人数:', hosyearweekcs[hos][week],\
                        hoszfy[hos][week], hosbxfy[hos][week]
                    resultm.append([keys, xmname[keys], projweekcost.sum(),\
                        hos,  hosypsum[hos], hosstd[hos][1], hosstd[hos][0],\
                          week,hosyearweekfy[hos][week], hosyearweekcs[hos][week] \
                         ,hoszfy[hos][week], hosbxfy[hos][week]])


        hostd = {}
        for hos, val in hosweekavg.iteritems():
            hostd[hos] = [val.std(),val.mean()]
            for week in val.index:
                if hosweekavg[hos][week] > 3*hostd[hos][0]+hostd[hos][1] and hosyearweekcs[hos][week] > 2:
                    resulta.append([keys,xmname[keys], projweekcost.sum()\
                        , hos, hosypsum[hos], hostd[hos][1],hostd[hos][0]\
                        , week, hosweekavg[hos][week],hosyearweekcs[hos][week], \
                        hoszfy[hos][week], hosbxfy[hos][week]])
        cellhos= {}
        for hos, val in hosweekavg.iteritems():
            celldata = []
            for wek in val.index:
                qshos = {}
                qshos['sj'] = str(wek)
                qshos['med'] = str(val[wek])
                qshos['avg'] = str(hosyearweekfy[hos][wek])
                celldata.append(qshos)
            cellhos[hos] = celldata
        cladata[keys] = cellhos


    columns_name = ['classid', 'classname', 'clacost', 'hos', 'clahoscost', 'avg', 'std', 'sj', 'med_cost', 'cnt', \
                    'zfy', 'bxfy']
    resultmde = pd.DataFrame(resultm, columns=columns_name)
    resultavg = pd.DataFrame(resulta, columns=columns_name)
    result = pd.merge(resultmde, resultavg, left_on=['classid', 'hos', 'sj'], right_on=['classid', 'hos', 'sj'], \
                      sort=False, suffixes=('', '_y'))
    result = result.rename(columns={'med_cost_y': 'avg_cost'})
    result.drop(['classname_y', 'clacost_y', 'clahoscost_y', 'avg_y', 'std_y', 'cnt_y',\
                 'zfy_y', 'bxfy_y'], axis='columns', inplace=True)

    w = Workbook()
    ws = w.add_sheet('sheet')
    out = "F:\classproject\Exresule.xls"
    j = 0
    for i in result.index:
        j += 1
        jsonMap = {"type": u"医院项目组使用异常波动", "common": None, "tabs": []}
        tabsMap = {"echartsType": "bar", "name": u"异常趋势图", "data": []}
        hosname, classid, sj, zfy, bxfy = str(result['hos'][i]), str(result['classid'][i]), str(result['sj'][i]), \
                                          str(result['zfy'][i]), str(result['bxfy'][i])
        commonParar = {}
        commonParar['classname'] = str(result['classname'][i])
        commonParar['clacost'] = str(result['clacost'][i])
        commonParar['hos'] = str(result['hos'][i])
        commonParar['clahoscost'] = str(result['clahoscost'][i])
        commonParar['avg'] = str(result['avg'][i])
        commonParar['std'] = str(result['std'][i])
        commonParar['sj'] = str(result['sj'][i])
        commonParar['med_cost'] = str(result['med_cost'][i])
        commonParar['avg_cost'] = str(result['avg_cost'][i])
        commonParar['zfy'] = str(result['zfy'][i])
        commonParar['bxfy'] = str(result['bxfy'][i])
        cdata = cladata[classid][hosname]
        tabsMap["data"] = cdata
        jsonMap["tabs"].append(tabsMap)
        jsonMap["common"] = commonParar
        finalResult = json.dumps(jsonMap, ensure_ascii=False)
        outputfile = "F:\classproject\Json%s%s%s.txt" % (sj, classid, j)
        f = open(outputfile, 'w')
        f.write(finalResult)
        f.close()

        hosname = hosname.decode('utf-8')

        m2 = hashlib.md5()
        m2.update(finalResult)
        md5 = m2.hexdigest()
        ws.write(i, 0, u'医院项目组使用异常波动')
        ws.write(i, 1, 24)
        ws.write(i, 2, 1)
        ws.write(i, 3, u'医院')
        ws.write(i, 6, '20170331')
        ws.write(i, 7, zfy)
        ws.write(i, 8, bxfy)
        ws.write(i, 9, 1)
        ws.write(i, 10, u'未调查')
        ws.write(i, 12, hosname)
        ws.write(i, 17, 2)
        ws.write(i, 21, finalResult)
        ws.write(i, 22, md5)
        ws.write(i, 23, 0)
    w.save(out)

if __name__ == '__main__':
    main()








