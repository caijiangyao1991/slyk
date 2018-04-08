# encoding: utf-8


import pandas as pd
from datetime import timedelta


def genCPMap(cy,ry,array):
    cyList = list(cy)
    ryList = list(ry)
    for eachCy in cyList:
        for eachRy in ryList:
            curCp = (eachCy, eachRy)
            array.append(curCp)


def nearPattern(arrList):
    map ={}
    size = len(arrList)
    print len(arrList)
    for curCp in arrList:
        if curCp in map.keys():
            map[curCp] += 1
        else:
            map[curCp] = 1
        # print size
        size -= 1
        print size

    f = open("./data/reslt.txt",'w')
    for each in map.keys():
        if(map[each]>1):
            f.write(str(each)+","+str(map[each])+"\n")

    print 'ok'
    f.close()


if __name__=="__main__":
    aDay = timedelta(days=1)
    #
    data = pd.read_csv('./data/borrowCard3_rawdata.csv' , encoding='gb18030')
    data_1 = data.rename(columns={'a3': 'hos_id', 'a4': 'hos_ma', 'a5': 'hos_name', 'a6': 'in_time', 'a7': 'out_time', 'a8': 'disease_id','a9': 'disease_name', 'a13': 'person_name', 'a14': 'person_id'})
    data_1['disease'] = data_1['disease_id'].str.extract('(\D\d\d)')
    #过滤疾病
    df = data_1.copy()
    # df = data_1[(data_1['hos_ma'] == 'F0002')]
    df = df[(df["disease"] != 'N19') | (df["disease"] != 'N18')]

    df['in_time'] = pd.to_datetime(df['in_time'])
    df['out_time'] = pd.to_datetime(df['out_time'])

    minCyDate = df['out_time'].min()
    maxCyDate = df['out_time'].max()

    cyDateSet = set(df['out_time'])
    size = len(cyDateSet)
    array = []

    for eachDay in cyDateSet:
        curCyPerson = set(df[df['out_time'] == eachDay]['person_id'])
        curRyPerson = set(df[(df['in_time'] == eachDay) | (df['in_time'] == eachDay + aDay)]['person_id'])
        #这一天出院和这一天入院的人两两组
        genCPMap(curCyPerson,curRyPerson,array)
        size -= 1
        print size
    nearPattern(array)





