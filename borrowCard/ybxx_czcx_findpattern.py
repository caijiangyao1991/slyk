# coding: utf-8
import pandas as pd
import numpy as np
from datetime import timedelta
pd.set_option('display.max_columns',None)

def genCPMap(cy,ry,array):
    cyList = list(cy)
    ryList = list(ry)
    for eachCy in cyList:
        for eachRy in ryList:
            curCp = (eachCy, eachRy)
            array.append(curCp)

def nearPattern(arrList,hos):
    map ={}
    size = len(arrList)
    print len(arrList)
    for curCp in arrList:
        if curCp in map.keys():
            map[curCp] += 1
        else:
            map[curCp] = 1
        size -= 1
        print size
    f = open("./data/ybxx/reslt_ybxx_" + str(hos) + ".txt",'w')
    for each in map.keys():
        if map[each]>1:
            f.write(str(each)+","+str(map[each])+"\n")
    print 'ok'
    f.close()



if __name__ =="__main__":
    aDay = timedelta(days=1)
    # TODO 将城乡和城职的数据合并　　
    data_cz = pd.read_csv(u'./data/城职.csv',encoding='gb18030')
    data_cx = pd.read_csv(u'./data/城乡.csv', encoding='gb18030')
    data = pd.concat((data_cz, data_cx), axis=0)
    data_1 = data.rename(columns={u'就诊登记号': 'hos_id', u'医院编码': 'hos_ma', u'医院名称': 'hos_name', u'入院时间': 'in_time', u'出院时间': 'out_time', u'第一出院诊断疾病编码': 'disease_id',u'第一出院诊断': 'disease_name', u'患者姓名': 'person_name', u'患者身份证': 'person_id'})
    data_1 = data_1[['hos_id','hos_ma','hos_name','in_time','out_time','disease_id','disease_name','person_name','person_id']]
    data_1['disease'] = data_1['disease_id'].str.extract('(\D\d\d)')

    #TODO 过滤看病次数小于3次的患者
    pidCount = data_1['hos_id'].groupby(data_1['person_id']).count().reset_index()
    pidlist = pidCount[pidCount['hos_id'] > 2]['person_id']
    data_1 = data_1[data_1.person_id.isin(pidlist)]

    # TODO 找到医院
    hosCount = data_1['hos_id'].groupby(data_1['hos_ma']).count().reset_index()
    hosCount.sort_values(by=['hos_id'], ascending=[False], inplace=True)
    hoslist = list(hosCount[hosCount['hos_id'] > 100]['hos_ma'])

    # #删除已经做过的
    # for i in [91001,91004,92001,92005]:
    #     hoslist.remove(i)

    # TODO 过滤疾病
    for hos in hoslist:
        print "hosma is %s" %hos
        df = data_1[data_1['hos_ma']==hos]
        hosname = list(df['hos_name'])[0]
        print "hosname is %s" %hosname
        df = df[(df["disease"] != 'N19') | (df["disease"] != 'N18')]

        #生成出入院时间连续的人的组合和次数
        df['in_time'] = pd.to_datetime(df['in_time'])
        df['out_time'] = pd.to_datetime(df['out_time'])
        minCyDate = df['out_time'].min()
        maxCyDate = df['out_time'].max()

        cyDateSet = set(df['out_time'])
        size = len(cyDateSet)

        array = []
        for eachday in cyDateSet:
            curCyPerson = set(df[df['out_time'] == eachday]['person_id'])
            curRyPerson = set(df[(df['in_time'] == eachday) | (df['in_time'] == eachday + aDay)]['person_id'])
            genCPMap(curCyPerson,curRyPerson,array)
            size -=1
            print size
        nearPattern(array,hos)

        # TODO 处理结果
        lat = []
        lon = []
        path2 = "./data/ybxx/reslt_ybxx_" + str(hos) + ".txt"
        f = open(path2, "r")
        lines = f.readlines()
        for eachline in lines:
            # 取出重叠次数大于2次的患者
            if int(eachline.split(',')[2]) > 2:
                lat.append(eachline.split(',')[0])
                lon.append(eachline.split(',')[1])
        data_2 = {'A': lat, 'B': lon}
        result = pd.DataFrame(data_2, columns=['A', 'B'])
        result['A'] = result['A'].astype(str).str.extract('(\d+)', expand=False)
        result['B'] = result['B'].astype(str).str.extract('(\d+)', expand=False)
        # 找到自己与自己时间重叠的数据
        result_1 = result[result['A'] != result['B']]
        lens = len(result_1)
        print "lens is %d" %lens
        if lens>0:
            result_1.to_csv("./data/ybxx/preResult_" + str(hos) + ".csv")

        # print "newlens is %d" %newlens
        # 排除重复的数据
        result_2 = result_1.drop_duplicates()
        # 过滤出这些病人的住院时间和疾病
        C = list(result_1.A) + list(result_1.B)
        value_list = set(C)
        df_person = df[df.person_id.isin(value_list)]

        # 获取重叠的时间
        cur = []
        for a, b in zip(result_2.A, result_2.B):
            for atime in df_person[df_person['person_id'] == a]['out_time']:
                for btime in df_person[df_person['person_id'] == b]['in_time']:
                    if timedelta(days=0) <= (btime - atime) <= timedelta(days=1):
                        att = [a, atime, b, btime]
                        cur.append(att)

        # 获取疾病
        array = set()
        arrayPerson = set()
        prePersonMap = {}
        postPersonMap = {}
        for eachline in cur:
            prePerson = eachline[0]
            preOutTime = eachline[1]
            postPerson = eachline[2]
            postInTime = eachline[3]
            adisease = df_person[(df_person['person_id'] == prePerson) & (df_person['out_time'] == eachline[1])]['disease']
            bdisease = df_person[(df_person['person_id'] == postPerson) & (df_person['in_time'] == eachline[3])]['disease']
            for adis, bdis in zip(adisease, bdisease):
                if adis == bdis:
                    arrayPerson.add((prePerson, postPerson))
                if (prePerson, postPerson) in arrayPerson:
                    array.add((prePerson, postPerson, preOutTime, postInTime))
                    if prePerson[0] not in prePersonMap.keys():
                        prePersonMap[prePerson] = 1
                    else:
                        prePersonMap[prePerson] += 1
                    if postPerson[0] not in postPersonMap.keys():
                        postPersonMap[postPerson] = 1
                    else:
                        postPersonMap[postPerson] += 1

        # 计算置信度
        confidence = 0.05
        preRotioMap = {}
        for eachKey in prePersonMap.keys():
            preRotioMap[eachKey] = float(prePersonMap[eachKey]) / len(df_person[(df_person['person_id'] == eachKey)])
        postRotioMap = {}
        for eachKey2 in postPersonMap.keys():
            postRotioMap[eachKey2] = float(postPersonMap[eachKey2]) / len(df_person[(df_person['person_id'] == eachKey2)])

        # 设定支持度阈值,并且输出json
        array1 = set()
        personMap = {}
        for eacharray in array:
            if (preRotioMap[eacharray[0]] > confidence) or (postRotioMap[eacharray[1]] > confidence):
                array1.add(eacharray)
        lens = len(array1)
        print "array1 is %s" %lens

        if lens>0:
            f = open('./data/ybxx/borrowCardResult_' + str(hos) + '.txt', 'w')
            f.write(str(array1))
            f.close()






