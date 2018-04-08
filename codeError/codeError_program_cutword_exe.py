#encoding=utf-8
__author__ = 'Administrator'
import jieba
import pandas as pd
import numpy as np
import time
import sys
import logging
import os
from configparser import ConfigParser
reload(sys)
sys.setdefaultencoding('utf-8')

class logger():
    def __init__(self, path):
        self.path = path
        self.logger = self.__ybxx(self.path)

    def __ybxx(self, path):
        path1 = path + u'\log.log'
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            filename='%s' % path1,
                            filemode='w+')
        # create logger
        logger_name = "codeError"
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
        nokeyword=str(item_name)
        print 'not find key word'
        # print nokeyword
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
    df=pd.read_table('./data/items_new.txt',header=None,encoding='utf8')
    df.columns=['name']
    namelist=list(df['name'].values)
    return namelist

def mkdir(path,log):
    # 引入模块
    isExists = os.path.exists(path)
    # 判断结果
    if not isExists:
        # 如果不存在则创建目录
        log.info('%s directory create success' % path)
        #print path + ' 创建成功'
        # 创建目录操作函数
        os.makedirs(path)
        return True
    else:
        # 如果目录存在则不创建，并提示目录已存在
        log.info('%s directory already exists' % path)
        #print path + ' 目录已存在'
        return False


if __name__=='__main__':
    #TODO 获取当前目录
    path = os.path.split(os.path.realpath(__file__))[0]
    path = path.decode('gbk')
    logger1 = logger(path)
    log = logger1.logger

    #TODO 开始运行
    log.info('run begin')

    #TODO 创建结果目录
    resultPath = path + u'\\result'
    mkdir(resultPath,log)

    #TODO 读取配置文件
    log.info('Read config file begin')
    cf = ConfigParser()
    localPath = path + u'\\config.ini'
    cf.read(localPath)
    fre = cf.getint('parameter','fre')
    log.info('Read configuration file end')

    #TODO 读取数据
    log.info('Read data begin')
    dataPath = path + u'\\data'
    listDir = os.listdir(dataPath)
    for i in range(len(listDir)):
        filePath = os.path.join(dataPath, listDir[i])
        if os.path.isfile(filePath):
            if 'items_new' in listDir[i]:
                items_new = pd.read_table(filePath ,header=None,encoding='utf8')
            else:
                data = pd.read_csv(filePath ,encoding='gb18030',dtype={u'就诊编码': np.str})
    log.info('Read rawData end')

    #TODO 分析开始
    jieba.load_userdict(dataPath + u"\\items_new.txt")
    data.rename(columns={u'就诊编码':'clinc',u'医保编码':'item_code',u'中心端名称':'item_name',u'医院端名称':'hos_name'},inplace=True)
    # TODO 预处理
    # 去掉 “特殊医疗器件、特殊治疗加收”“其他推拿治疗”“特殊医疗器件、特殊治疗加收5%”“一般诊疗服务项目（乡、中心卫生院）”
    data1 = data[
        (data['item_code'] != 'ZLZF000000000') & (data['item_code'] != 'YYZF000000')& (data['item_code'] != 'YP00000000') & (data['item_code'] != 'YPZF00000000')& (data['item_code'] != 'YPJXJ0000000')
        & (data['item_code'] != 'YPXJ00000000') & (data['item_code'] != 'YPJ0000000')& (data['item_code'] != 'ZL100000000') & (data['item_code'] != 'ZL150000000')
        & (data['item_code'] != 'ZL450000009') & (data['item_code'] != 'ZL050000000')& (data['item_code'] != 'ZL331004020') & (data['item_code'] != 'ZL11110000101')& (data['item_code'] != 'ZL120100014')
        ]
    #去掉没有编码错误的
    data1 = data1[data1['item_name'] != data1['hos_name']]
    #排除同一次就诊中，中心端名称、医保端名称、医保编码都一样的
    data1.drop_duplicates(subset=['clinc','item_code','item_name','hos_name'], inplace=True)

    #TODO 查找错误编码，并存储没有找到关键词的名称
    res_df, noKey = fun_no_name(data1)
    noKey.to_excel(resultPath + u'\\noKey.xlsx', index=False)

    # TODO 过滤掉发生错误次数较少的
    data2 = data1['clinc'].groupby([data1['item_name'],data1['hos_name']]).count().reset_index()
    data2.rename(columns={'clinc':'fre'},inplace=True)
    data3 = pd.merge(res_df,data2, left_on=['item_name','wrong_name'], right_on=['item_name','hos_name'],how='left')
    data3 = data3[data3['fre']>fre]
    data3.to_excel(resultPath + u'\\res.xlsx',index=False)
    log.info('Data Analyse end')



