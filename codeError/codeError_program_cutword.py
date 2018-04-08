#encoding=utf-8
__author__ = 'Administrator'
import jieba
import pandas as pd
import numpy as np
import time

def find_key_word(item_name,nameList):
    res=jieba.cut(item_name)
    # keyword=''
    wordList=[]
    nokeyword = ''
    print 'cut begin'
    lenlist= []
    for each in res:
        print each
        if each in nameList:
            lens = len(each)
            lenlist.append(lens)
            print each
            wordList.append(each)
    if len(wordList)==0:
        nokeyword=item_name.decode('gbk')
        print 'not find key word:' + nokeyword
    # print 'keyword'
    # print wordList
    print 'cut end'
    return wordList, nokeyword

def find_wrong_names(exa_df,keyword):
    wrongList=[]
    for index,row in exa_df.iterrows():
        hos_name=row['hos_name'].strip()
        print hos_name
        rightlist = []
        for eachword in keyword:
            print 'keyword :'+eachword
            if eachword in hos_name:
                rightlist.append(eachword)
        if len(rightlist)==0:
            print 'wrong code:'+hos_name
            wrongList.append(hos_name)
    return wrongList

def fun_no_name(itemDf):
    wrongMap={'item_name':[],'wrong_name':[]}
    noKey = {'noKey':[]}
    namelist=get_name_list()
    itemNameList=itemDf['item_name'].drop_duplicates()
    for eachitem in itemNameList:
        print eachitem
        sub_df=itemDf[itemDf['item_name']== eachitem]
        curDf=sub_df.groupby(['item_name','hos_name']).size().reset_index()
        key, nokey =find_key_word(eachitem,namelist)
        if len(key) ==0:
            noKey['noKey'].append(nokey)
            continue
        wrongList=find_wrong_names(curDf,key)
        for eachWrong in wrongList:
            wrongMap['item_name'].append(eachitem)
            wrongMap['wrong_name'].append(eachWrong)
    res_df = pd.DataFrame(wrongMap)
    noKey_df = pd.DataFrame(noKey)
    return res_df, noKey_df



def get_name_list():
    df = pd.read_table('./data/items_new.txt',header=None,encoding='utf8')
    df.columns=['name']
    namelist=list(df['name'].values)
    return namelist


if __name__=='__main__':
    print ('start time:', time.asctime((time.localtime(time.time()))))
    # jieba.load_userdict("./data/items_new.txt")
    data=pd.read_csv(u'./data/城乡.csv',encoding='gb18030',dtype={u'就诊编码': np.str})
    print data.head()
    # # data = data[[u'就诊编码',u'医保编码',u'中心端名称',u'医院端名称']]
    # # # TODO 预处理
    # # # 去掉 “特殊医疗器件、特殊治疗加收”“其他推拿治疗”“特殊医疗器件、特殊治疗加收5%”“一般诊疗服务项目（乡、中心卫生院）”
    # # data1 = data[
    # #     (data[u'医保编码'] != 'ZLZF000000000') & (data[u'医保编码'] != 'YYZF000000')
    # #     & (data[u'医保编码'] != 'YP00000000')& (data[u'医保编码'] != 'YPZF00000000')
    # #     &(data[u'医保编码'] != 'YPJXJ0000000')
    # #     & (data[u'医保编码'] != 'YPXJ00000000') & (data[u'医保编码'] != 'YPJ0000000')
    # #     & (data[u'医保编码'] != 'ZL100000000')& (data[u'医保编码'] != 'ZL150000000')
    # #     & (data[u'医保编码'] != 'ZL450000009')& (data[u'医保编码'] != 'ZL050000000')
    # #     & (data[u'医保编码'] != 'ZL331004020')& (data[u'医保编码'] != 'ZL11110000101')
    # #     & (data[u'医保编码'] != 'ZL120100014')
    # #     ]
    # # data1.columns=['clinc','item_code','item_name','hos_name']
    # # #去掉没有编码错误的
    # # data1 = data1[data1['item_name'] != data1['hos_name']]
    # # #排除同一次就诊中，中心端名称、医保端名称、医保编码都一样的
    # # data1.drop_duplicates(subset=['clinc','item_code','item_name','hos_name'], inplace=True)
    # # data1.to_csv(u'./data/城乡_new.csv',encoding='gb18030',index=False)
    # data1 = pd.read_csv(u'./data/城乡_new.csv', encoding='gb18030', dtype={'clinc': np.str})
    #
    # #TODO 查找错误编码，并存储没有找到关键词的名称
    # res_df, noKey = fun_no_name(data1)
    # noKey.to_excel('./outdata/noKey.xlsx', index=False)
    #
    # # TODO 过滤掉发生错误次数较少的
    # data2 = data1['clinc'].groupby([data1['item_name'],data1['hos_name']]).count().reset_index()
    # data2.rename(columns={'clinc':'fre'},inplace=True)
    # data3 = pd.merge(res_df,data2, left_on=['item_name','wrong_name'], right_on=['item_name','hos_name'],how='left')
    # data3 = data3[data3['fre']>10]
    # print len(data3)
    # data3.to_excel('./outdata/res.xlsx',index=False)
    # print ('end time:', time.asctime((time.localtime(time.time()))))


