# coding:utf-8
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from dtw import dtw
import numpy as np
from numpy.linalg import norm
plt.rcParams['font.sans-serif'] = ['Simhei'] # TODO 画图显示中文
plt.rcParams['axes.unicode_minus'] = False   # TODO 显示负号

def dis(a, b):
    x = np.array(a).reshape(-1, 1)
    y = np.array(b).reshape(-1, 1)
    dist, cost, acc, path = dtw(x, y, dist=lambda x, y: norm(x - y))
    return dist

def calcuDistance(a, b):
    #x = np.array(a).reshape(-1, 1)
    #y = np.array(b).reshape(-1, 1)
    dist, cost, acc, path = dtw(a, b, dist=lambda x, y: norm(x - y))
    distance1 = np.sqrt(np.sum(np.square(a - b)))
    distance2 = np.sqrt(np.sum(np.abs(a - b)))
    maxValues = np.max(b-a)
    return maxValues, dist, list((b-a)).index(maxValues)+1


def age_change(age):
    #age = int(age)
    if age>=0 and age<=6:
        return 1
    elif age>=7 and age<=17:
        return 2
    elif age>=18 and age<=28:
        return 3
    elif age>=29 and age<=40:
        return 4
    elif age>=41 and age<=65:
        return 5
    elif age>=66 and age<=72:
        return 6
    elif age>=73 and age<=84:
        return 7
    else:
        return 8

def get_data():
    # con = psycopg2.connect(host='192.168.10.60',port=5432,password='gpadmin',user='gpadmin',database='dy')
    # sql = """SELECT  kc21_aaz217 as jzid,KC21_AKC196 as illness,KB01_CKZ543 as hosid,KB01_CKB519 as hosname,SKC17_nl as age,KB01_AKA101 as hoslevel
    # FROM t_jsxx_bc where KC21_AKC196 <>'' and kc21_jz_name='住院' and ydjy=0  and SKC17_nl<>''  and to_char(KC21_CKC538,'YYYY')='2016'
    #  """
    # df = pd.read_sql(sql, con)
    # # TODO 处理疾病按照"."和" "分割
    # df['illness'] = df['illness'].apply(lambda a:a.split('.')[0])
    # df['illness'] = df['illness'].apply(lambda a: a.split(' ')[0])
    # df['jzid'] = df['jzid'].astype('str')
    df = pd.read_csv(u'F:\疾病年龄段分布异常算法\数据.csv')
    hosNameData = df.ix[:, ['hosname','hosid']].drop_duplicates()
    #hosNameData = hosNameData.drop_duplicates()
    hosName = {}
    for ind in hosNameData.index:
        hosid = hosNameData.loc[ind,'hosid']
        hosNa = hosNameData.loc[ind,'hosname']
        hosName[hosid] = hosNa if hosid not in hosName else hosNa
    #print df.head()
    # # TODO 把年龄处理成年龄段的形式
    # for ind in df.index:
    #     age = int(df.loc[ind, 'age'])
    #     df.loc[ind, 'age'] = age_change(age)
    # print df.head()
    #df.to_csv(u'F:\疾病年龄段分布异常算法\数据.csv',index_label=None)
    #df = pd.read_csv(u'F:\疾病年龄段分布异常算法\数据.csv')
    return df, hosName

def supplement(hosAgeData):
    ageList = [1, 2, 3, 4, 5, 6, 7, 8]
    for age in ageList:
        if age not in hosAgeData.index:
            hosAgeData[age] = 0
    return hosAgeData

def increaceRatio(allAgeRatio):
    ratio = []
    for ind, val in enumerate(allAgeRatio.values[:-1]):
        try:
            rata = (allAgeRatio[ind+2]-val)/(1 +.0)
            ratio.append(rata)
        except ZeroDivisionError:
            ratio.append(0)
    return ratio

def age_Name():
    """
    '1-->'0-6岁'
    '2-->'7-17岁'
    '3-->'18-28岁'
    '4-->'29-40岁'
    '5-->'41-65岁'
    '6-->'66-72岁'
    '7-> '73-84岁'
    '8-->'85岁以上'
    :return:
    """
    ageName = {}
    ageName[1] = '0-6岁'
    ageName[2] = '7-17岁'
    ageName[3] = '18-28岁'
    ageName[4] = '29-40岁'
    ageName[5] = '41-65岁'
    ageName[6] = '66-72岁'
    ageName[7] = '73-84岁'
    ageName[8] = '85岁以上'
    return ageName

def main():
    j = 0
    ageName = age_Name()
    #dw = -888888
    df, hosName = get_data()
    #disease = df['illness'].drop_duplicates()
    #print '***************'
    diseaseCount = df['jzid'].groupby(df['illness']).count()
    diseaseList = diseaseCount[diseaseCount >= 500]
    for disease in diseaseList.index:
        #disease = 'J44'
        diseaseData = df[df['illness'] == disease]
        #print len(diseaseData['hosid'].drop_duplicates())
        #hosLevelData = diseaseData['hoslevel'].drop_duplicates()
        # for hoslevel in hosLevelData:
        #     #dSerise = []
        #     diseaseLevelData = diseaseData[diseaseData['hoslevel'] == hoslevel]
        # TODO 计算总体
        allAge = diseaseData['jzid'].groupby(diseaseData['age']).count()

        allAgeRatio = allAge/allAge.sum()
        allAgeRatio = supplement(allAgeRatio)
        allAgeRatio = allAgeRatio.sort_index()
        #allAgeRatioInC = increaceRatio(allAgeRatio)
        hosCount = diseaseData['jzid'].groupby(diseaseData['hosid']).count()
        hosList = hosCount[hosCount >= 50]
        for hos in hosList.index:
            if hos in ['F0043', 'F0017', 'F0102', 'F05502', 'F0007','F0112']:
                continue
            hosAgeData = diseaseData[diseaseData['hosid'] == hos]
            hosAge = hosAgeData['jzid'].groupby(hosAgeData['age']).count()
            hosAge = supplement(hosAge)
            hosAgeRatio = hosAge/hosAge.sum()
            hosAgeRatio = hosAgeRatio.sort_index()
            disValues, dtwdis, anomalyAge = calcuDistance(allAgeRatio.values, hosAgeRatio.values)
            if dtwdis > 0.03 and disValues > 0.45:
                j += 1
                try:
                    hosName[hos] = hosName[hos].decode('utf-8')
                except UnicodeEncodeError:
                    pass
                x = range(8)
                plt.plot(x, allAgeRatio, 'k--o', label=u'总体')
                plt.plot(x, hosAgeRatio, label=hosName[hos])
                xtick = [u'0-6岁', u'7-17岁', u'18-28岁', u'29-40岁', u'41-65岁', u'66-72岁', u'73-84岁', u'85岁以上']
                plt.xticks(x, xtick, rotation=0)
                print disValues, hos, 2016, ageName[anomalyAge], hosName[hos], disease, dtwdis,hosAge.sum(), allAge.sum()
                plt.title(hosName[hos]+disease)
                plt.legend()
                pictureName = hos +"-" + disease
                #plt.savefig(u'F:\疾病年龄段分布异常算法\\0.3\%s%d.png'% (pictureName, j))
                plt.show()
                plt.close()

                # jsonMap = {"type": None, "common": None, "tabs": []}
                # tabsMap = {"echartsType": "bar", "echartsUnit": "%", "name": None, "data": []}
                # commonParar = {}
                # commonParar['anomalyYear'] = str(2016)
                # commonParar['hospitalName'] = hosName[hos]
                # commonParar['ageRange'] = ageName[anomalyAge]











if __name__ == '__main__':
    """
    '1-->'0-6岁'
    '2-->'7-17岁'
    '3-->'18-28岁'
    '4-->'29-40岁'
    '5-->'41-65岁'
    '6-->'66-72岁'
    '7-> '73-84岁'
    '8-->'85岁以上'

    1：对所有的疾病
    2：判断疾病数总数大于50（暂时)，单独每家医院疾病大于50（暂时）
    3：对符合的疾病  计算得到各个年龄段总序列，每个医院的序列
    4：对于总体序列相差过大的医院序列提取出来，当做异常值来处理（阀值待定）
    """
    main()


