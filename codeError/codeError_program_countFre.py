# coding: utf-8
import pandas as pd
import numpy as np
import sys
import collections
import re
reload(sys)
sys.setdefaultencoding('utf8')
pd.set_option('display.max_columns',None)


def readData(path):
    data = pd.read_csv(path, encoding='gb18030',
                       dtype={u'药品剂型': np.str, u''
                                               u'#分类名称': np.str, u'中心端名称': np.str, u'就诊登记号': np.str})
    print len(data)
    # TODO 排除中心端名称和医保端名称完全一样的
    data1 = data[data[u'中心端名称'] != data[u'医院端名称']]
    # TODO 过滤自费诊疗服务诊疗项目\自费材料\未入库自费药品\未入库的乡级目录可报销药品（甲类）\未入库的乡级目录可报销药品（乙类）\未入库的县级目录可报销药品（甲类）
    data2 = data1[
        (data1[u'医保编码'] != 'ZLZF000000000') & (data1[u'医保编码'] != 'YYZF000000')
        & (data1[u'医保编码'] != 'YP00000000')& (data1[u'医保编码'] != 'YPZF00000000')
        &(data1[u'医保编码'] != 'YPJXJ0000000')
        & (data1[u'医保编码'] != 'YPXJ00000000') & (data1[u'医保编码'] != 'YPJ0000000')
        ]
    # TODO 排除同一次就诊中中心端名称、医保端名称、医保编码都一样的
    data2.drop_duplicates(subset=[u'就诊编码', u'医保编码', u'中心端名称', u'医院端名称'], inplace=True)
    return data2

def remove_noise(words):
    noise_pattern = re.compile("|".join(["(\()(.+?)(\))",U"（.+?）",U"(\()(.+?)）",
                                         U"（(.+?)(\))","(\[)(.+?)(\])","^[○]","^[□▲]","^[◆]","^[☆]",
                                         "^[△]","^[●]","^[#]","^[※]","^[■]","^[▲]","^[▲]","^[*]",
                                         "[◆]$","[★]$","[○]$","[▲△]$","[▲]$","[*]$","[!]$"]
                                        )) #去掉括号所有的噪声
    clean_words = re.sub(noise_pattern, "", words)
    return clean_words
# words = u'灸法◆'
# print remove_noise(words)

def flat(df):
    reslist = []
    for each in df:
        reslist.append(each)
    return reslist

#词频统计 出现次数小于15次
def count(nums,i):
    c1 = collections.Counter(nums)
    res = c1.most_common()[i:(i-1):-1] #取倒数第几小的频数
    result = []
    for i in res:
        if i[1] < 15:
            result.append(i)
    return result

if __name__=='__main__':
    path = u'./data/城乡.csv'
    data = readData(path)
    data.to_csv(u'./data/城乡_new.csv',encoding='gb18030')
    print data.head()
    #
    # #去掉一些可能干扰的东西
    # data[u'医院端名称_new'] = data[u'医院端名称'].apply(lambda x: remove_noise(x))
    # data[u'医院端名称_new'] = data[u'医院端名称_new'].apply(lambda x: x.strip())
    # #再排除一次，去噪声后，新的医院端名称和中心端名称相同的
    # data = data[data[u'医院端名称_new'] != data[u'中心端名称']]
    #
    # # TODO 找到每个医保编码对应的中心端名称和医院端名称
    # centreName = data.drop_duplicates(subset=u'医保编码')
    # centreName = centreName[[u'医保编码', u'中心端名称']]
    # hosName = data[u'医院端名称_new'].groupby(data[u'医保编码']).apply(flat).reset_index()
    # name = pd.merge(centreName, hosName, on=u'医保编码', how='left')
    # print name.head()
    #
    # # # TODO 统计医院端名称中各词出现的频次， 只取其中出现次数倒数第一小和倒数第二小的（都要小于20，否则W为空）
    # flag1 = []  #存倒数第一小的词和词频
    # flag2 = []  #存倒数第二小的词和词频
    # for i in range(len(name)):
    #     nums = list(name[u'医院端名称_new'])[i]
    #     result1 = count(nums,-1)
    #     result2 = count(nums,-2)
    #     flag1.append(result1)
    #     flag2.append(result2)
    # name['flag1'] = flag1
    # name['flag2'] = flag2
    #
    # #去掉最小频次为空的
    # lens = []
    # for i in range(len(name)):
    #     j = list(name['flag1'])[i]
    #     lens.append(len(j))
    # name['lens'] = lens
    # name_res = name[name['lens'] > 0]
    # print name_res.head()
    # print(len(name_res))
    # name_res1 = name_res[[u'医保编码',u'中心端名称','flag1','flag2']]
    # name_res1.to_csv('./outdata/res_cx_4.csv',encoding='gb18030')
