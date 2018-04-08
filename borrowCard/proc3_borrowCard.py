# encoding: utf-8

# 在已经查找出具有时间连续特征的病人的基础上，求是否具有

import pandas as pd
import json
from datetime import timedelta

hos = 'F0001'
confidence = 0.05
# 读取原始数据，选出医院F0001
path = './data/borrowCard3_rawdata.csv'


def readrawdata(path):
    data = pd.read_csv(path, encoding='gb18030')
    data_1 = data.rename(columns={'a3': 'hos_id', 'a4': 'hos_ma', 'a5': 'hos_name', 'a6': 'in_time', 'a7': 'out_time',
                                  'a8': 'disease_id', 'a9': 'disease_name', 'a13': 'person_name', 'a14': 'person_id'})
    data_1['disease'] = data_1['disease_id'].str.extract('(\D\d\d)')
    df = data_1[(data_1['hos_ma'] == hos)]
    # 时间变量变成日期格式
    df['in_time'] = pd.to_datetime(df['in_time'])
    df['out_time'] = pd.to_datetime(df['out_time'])
    return df


# 读取已经找出的具有时间连续特征的病人，同时过滤出这些病人的住院时间和疾病等数据
lat = []
lon = []
path2 = './data/reslt.txt'


def readpatientdata(path2, df):
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
    samePeople = result[result['A'] == result['B']]['A']
    result_1 = result[(~result.A.isin(samePeople)) & (~result.B.isin(samePeople))]
    # 排除重复的数据
    result_2 = result_1.drop_duplicates()
    # 过滤出这些病人的住院时间和疾病
    C = list(result_1.A) + list(result_1.B)
    value_list = set(C)
    df_person = df[df.person_id.isin(value_list)]
    return result_2, df_person


# 获取重叠的时间
cur = []


def gettime(result_2, df_person):
    for a, b in zip(result_2.A, result_2.B):
        for atime in df_person[df_person['person_id'] == a]['out_time']:
            for btime in df_person[df_person['person_id'] == b]['in_time']:
                if timedelta(days=0) <= (btime - atime) <= timedelta(days=1):
                    att = [a, atime, b, btime]
                    cur.append(att)
    return cur


# 获取疾病

def getdisease(cur, df_person):
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
        preRotioMap = {}
        for eachKey in prePersonMap.keys():
            preRotioMap[eachKey] = float(prePersonMap[eachKey]) / len(df_person[(df_person['person_id'] == eachKey)])
        postRotioMap = {}
        for eachKey2 in postPersonMap.keys():
            postRotioMap[eachKey2] = float(postPersonMap[eachKey2]) / len(
                df_person[(df_person['person_id'] == eachKey2)])

    # 设定支持度阈值,并且输出json
    array1 = set()
    personMap = {}
    for eacharray in array:
        if (preRotioMap[eacharray[0]] > confidence) or (postRotioMap[eacharray[1]] > confidence):
            array1.add(eacharray)
    for each in array1:
        if str(each[0]) + ',' + str(each[1]) in personMap.keys():
            personMap[str(each[0]) + ',' + str(each[1])].append(each)
        else:
            newarraylist = []
            personMap[str(each[0]) + ',' + str(each[1])] = newarraylist
            newarraylist.append(each)

    for person, personTime in personMap.items():
        jsonMap = {"type": u"出借医保卡", "common": None, "tabs": []}
        commonParar = {}
        personA = list(df_person[df_person['person_id'] == personTime[0][0]]['person_name'])[0]
        personB = list(df_person[df_person['person_id'] == personTime[0][1]]['person_name'])[0]
        commonParar['involvedPerson'] = personA + "," + personB
        commonParar['hospitalName'] = "德阳新铁医院"
        commonParar['timeContinueNum'] = len(personTime)
        jsonMap['common'] = commonParar

        FigureMap = {"name": u"出借医保卡", "echartsType": "bar", "echartsUnit": "%", "data": []}
        name = {"name": "涉及单据"}
        tableArray = []
        tableMap = {}
        for eachlist in personTime:
            # print(eachlist)
            tableMap['prePatientName'] = personA
            tableMap['postPatientName'] = personB
            tableMap['outtime'] = str(eachlist[2])
            tableMap['intime'] = str(eachlist[3])
            tableArray.append(tableMap)
            tableMap = {}

        FigureMap['data'] = tableArray
        jsonMap['tabs'].append(FigureMap)
        jsonMap['tabs'].append(name)

        finalResult = json.dumps(jsonMap)
        print(finalResult)

        f = open('d://borrowCardJson.txt', 'w')
        f.write(finalResult)
        f.close()

    f = open('./data/borrowCardResult' + str(hos) + '.txt', 'w')
    f.write(str(array1))
    f.close()


if __name__ == '__main__':
    rawdata = readrawdata(path)
    patientdata, patientrawdata = readpatientdata(path2, rawdata)
    timedata = gettime(patientdata, patientrawdata)
    diseasedata = getdisease(timedata, patientrawdata)
