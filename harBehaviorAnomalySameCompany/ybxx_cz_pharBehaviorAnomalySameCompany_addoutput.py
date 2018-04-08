# coding: utf-8

import time
from functools import reduce
import sys
import fp_growth
import numpy as np
import pandas as pd
import os
import logging
from configparser import ConfigParser
from sqlalchemy import create_engine
import shortuuid
reload(sys)
sys.setdefaultencoding('gbk')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK'

pd.set_option('display.max_columns', None)

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
        #创建logger
        logger_name = "pharBehaviorAnomalySameCompany_addoutput"
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)

        #create stream handler
        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        #create format
        fmt = "%(asctime)-15s %(levelname)s %(filename)s %(lineno)d  %(message)s"
        datefmt = "%a %d %b %Y %H:%M:%S"
        formatter = logging.Formatter(fmt, datefmt)
        # add handler and formatter to logger
        sh.setFormatter(formatter)
        logging.getLogger('').addHandler(sh)
        return logger
def mkdir(path, log):
    import os
    isExists = os.path.exists(path)
    if not isExists:
        #如果不存在则创建目录
        log.info('%s directory create success' %path)
        os.makedirs(path)
        return True
    else:
        #如果目录存在则不创建，并提示目录已存在
        log.info('%s directory already exists' %path)
        return False

def readdata(path):
    df = pd.read_csv(path, encoding='gb18030',dtype={u'就诊登记号':np.str,u'患者身份证':np.str,u'医院编码':np.str})
    df['intime'] = df[u'入院时间'].apply(lambda x: pd.to_datetime(x).date())
    df['outtime'] = df[u'出院时间'].apply(lambda x: pd.to_datetime(x).date())
    df['intime'] = df['intime'].astype(str)
    df['phar_id'] = df[u'医院编码'].astype(str)
    df['indv_id'] = df[u'患者身份证'].astype(str)
    df['cli_id'] = df[u'就诊登记号'].astype(str)
    df['company'] = df[u'患者工作单位']
    # 过滤掉公司为空的记录
    df = df[df.company.notnull()]
    df = df[(df['company'] !=u'基本养老、医疗保险个人缴费')&(df['company'] !=u'个体人员自缴')&(df['company'] !=u'退休不缴费人员基本医疗保险专户单位')]
    #过滤掉有家精神病，青白江区大同镇卫生院701004,成都锦欣精神病医院095006,成都市天府新区精神病医院093085,
    # 省内大成都外的省级及以上医院90601,省内大成都外的县（市）级医院90603,省内大成都外的地（市）级医院90602,省内大成都外的乡镇及以下医院090604
    #省外省级及以上医院090701,省外县(市)级医院090703
    #成都市第四人民医院（成都市精神卫生中心）092004,成都市温江区第三人民医院095025,金堂县精神病医院095027成都市精神病院095005
    df_1 = df[~df.phar_id.isin(['701004','90601','90603','90602','095006','093085','090604','092004','095025','095027','090701','090703','095005','870012'])]
    # #过滤掉去医院次数小于频繁项数目的，先暂时过滤掉小于3次的人
    cliCount = df_1[u'cli_id'].groupby(df['indv_id']).count().reset_index()
    cliCount.rename(columns={'cli_id':'cliCount'},inplace=True)
    df_1 = pd.merge(df_1,cliCount,on='indv_id', how='left')
    df_2 = df_1[df_1['cliCount']>3]
    df_2['indvphartm'] = df_2['indv_id'] + df_2['phar_id'] + df_2['intime']
    df_2['phartmcomp'] = df_2['phar_id'] + df_2['intime']+df_2['company']
    df_3 = df_2.drop_duplicates(subset='indvphartm')
    df_3 = df_3[['indv_id', 'phartmcomp']]
    print df_3.head()
    # df_3.to_csv('./data/chenzhengData_samecompany.csv',index=False,encoding='gb18030')
    return df, df_3


def flat(df):
    return reduce(lambda x, y: str(x) + ',' + str(y), df)


# 过滤掉小于1的
def getitems(df_1):
    items = df_1['indv_id'].groupby(df_1['phartmcomp']).apply(flat)
    print items.head()
    items_1 = items.astype(str)
    print(len(items_1))
    lists = []
    for i in range(len(items_1)):
        if len(items_1[i]) > 1:
            lists.append(items_1[i])
    return lists


def main():
    # TODO 获取当前目录
    path = os.path.split(os.path.realpath(__file__))[0]
    path.decode('gbk')
    # 实例化
    logger1 = logger(path)
    # 调用实例化里面的函数
    log = logger1.logger
    # TODO 开始运行
    log.info('run begin')

    # TODO 创建结果目录
    resultPath = path + u'\\result'
    mkdir(resultPath, log)

    # TODO 读取配置文件
    log.info('read configuration file begin')
    cf = ConfigParser()
    localPath = path + u'\\config.ini'
    cf.read(localPath)
    freqs = cf.getint('parameter', 'freqs')
    username = cf.get('database', 'username')
    password = cf.get('database','password')
    tns = cf.get('database', 'tns')
    databasename = cf.get('database', 'databasename')
    table = cf.get('database', 'table')
    tabledetail = cf.get('database', 'tabledetail')
    log.info("Read configuration file end")

    # TODO 读取数据
    log.info('Read data begin')
    datapath = path + u'\\data'
    listDir = os.listdir(datapath)
    filePath = os.path.join(datapath,listDir[0])
    df, rawdata = readdata(filePath)
    log.info('Read Data end')

    # TODO 开始分析
    items = getitems(rawdata)
    print(len(items))
    transactions = [line.split(',') for line in items]
    result = []
    itemsets = []
    for itemset, support in fp_growth.find_frequent_itemsets(transactions, freqs, True):
        result.append((itemset, support))
    results = sorted(result, key=lambda i: len(i[0]), reverse=True)

    # 去除频繁1项
    resl = []
    for itemset, support in results:
        if len(itemset) > 1:
            resl.append((itemset, support))
    # print resl
    print(len(resl))

   # 去除所有子集
    res1 = sorted(resl, key=lambda i: len(i[0]), reverse=True)
    res2 = [resl[0][0]]
    frItems = [resl[0]]
    for i, support in resl:
        TF = []
        for j in res2:
            TF.append(str(np.in1d(i, j).all()))
        if 'True' in TF:
            continue
        else:
            res2.append(i)
            frItems.append((i, support))
    print(len(frItems))
    print frItems

    lists = []
    for index,(i,support) in enumerate(frItems):
        df_person = df[df['indv_id'].isin(i)]
        df_person['patientCount'] = len(i)
        df_person['id'] = shortuuid.uuid()
        df_person['preNum'] = len(i)
        #找出频繁一起入院的时间
        time = df_person['indv_id'].groupby(df_person['intime']).count().reset_index()
        time.rename(columns={'indv_id':'times'},inplace=True)
        presonNum = len(i)-1
        time = time[time['times']>presonNum]
        timelist = time['intime']
        df_person = df_person[df_person['intime'].isin(timelist)]
        df_person['sumCost'] = sum(df_person[u'医疗总费用'])
        df_person['sumClaimCost'] = sum(df_person[u'报销金额'])
        df_person['fre'] = len(df_person)/len(i)
        lists.append(df_person)
        i.append(support)
    result = pd.concat(lists)
    result['department']= ''
    result['doctor'] = ''
    print result.head()

    # #TODO 输出
    filename1 = u'\\su_illegal_seek_exception.csv'
    filename2 = u'\\su_illegal_seek_exception_detail.csv'
    outputfile1 = resultPath + filename1
    outputfile2 = resultPath + filename2

    # #主表输出
    res1 = result[['id',u'患者姓名']].drop_duplicates()
    res1 = res1[u'患者姓名'].groupby(res1['id']).apply(lambda x: ','.join(x)).reset_index()
    res2 = result[['id', 'intime']].drop_duplicates()
    res2 = res2['intime'].groupby(res2['id']).apply(lambda x: ','.join(x)).reset_index()
    result2 = pd.merge(res1,res2,on='id',how='left')
    res3 = result[['id', u'医院名称']].drop_duplicates()
    res3 = res3[u'医院名称'].groupby(res3['id']).apply(lambda x: ','.join(x)).reset_index()
    result2 = pd.merge(result2, res3, on='id', how='left')
    res4 = result[['id',u'医院名称', u'医院所在分中心',]].drop_duplicates()
    res4 = res4[u'医院所在分中心'].groupby(res4['id']).apply(lambda x: ','.join(x)).reset_index()
    result2 = pd.merge(result2, res4, on='id', how='left')
    res5 = result[['id', u'医院等级',u'医院名称']].drop_duplicates()
    res5 = res5[u'医院等级'].groupby(res5['id']).apply(lambda x: ','.join(x)).reset_index()
    result2 = pd.merge(result2, res5, on='id', how='left')

    result2.rename(columns={u'患者姓名':'involvedPatientNames', 'intime':'inHosTime',u'医院名称':'hospitalName',u'医院等级':'hospitalLevel',u'医院所在分中心':'hospitalArea'},inplace=True)
    result3 = result[['id','sumCost','sumClaimCost','doctor','fre','patientCount']]

    su_illegal_seek_exception = pd.merge(result2,result3,left_on='id',right_on='id',how='left')
    su_illegal_seek_exception.drop_duplicates(inplace=True)
    su_illegal_seek_exception.rename(columns={'sumCost':'allMoney','sumClaimCost':'bcMoney','doctor':'doctorName','fre':'frequency'},inplace=True)
    su_illegal_seek_exception.to_csv(outputfile1,encoding='gb18030',index=False)
    # to database
   #  log.info('Creat database engine')
   #  engine = create_engine('mysql+mysqldb://%s:%s@%s/%s?charset=utf8'  %(username,password,tns,databasename ))
   #
   #  su_illegal_seek_exception.to_sql(table, con=engine, if_exists='append',index=False)
   #
   #
   #  #明细表输出
   #  su_illegal_seek_exception_detail = result[['id',u'患者身份证',u'就诊登记号',u'个人编码',u'患者工作单位','department',
   #  u'第一出院诊断疾病编码',u'第一出院诊断',u'医疗总费用',u'报销金额',u'医院所在分中心',u'医院编码',u'医院等级',u'医院名称',
   #  'doctor',u'患者姓名','intime','outtime']]
   #
   #  su_illegal_seek_exception_detail.rename(columns={'id':'mainId',u'患者身份证':'idcard',u'就诊登记号':'jzId',u'个人编码':'sbId',u'患者工作单位':'company',
   # u'第一出院诊断疾病编码':'mainZdCode',u'第一出院诊断':'mainZdName',u'医疗总费用':'totalMoney',u'报销金额':'bxMoney',
   #  u'医院所在分中心':'hospitalArea', u'医院编码':'hospitalCode',u'医院等级':'hospitalLevel',u'医院名称':'hospitalName',
   #  'doctor':'doctorName',u'患者姓名':'patientName','intime':'inHosTime', 'outtime':'outHosTime'},inplace=True)
   #  # su_illegal_seek_exception_detail
   #  idlist = []
   #  for i in range(len(su_illegal_seek_exception_detail)):
   #      idlist.append(shortuuid.uuid())
   #  su_illegal_seek_exception_detail['id']=idlist
   #
   #  su_illegal_seek_exception_detail.to_csv(outputfile2, encoding='gb18030',index=False)
   #
   #  su_illegal_seek_exception_detail.to_sql(tabledetail, con=engine, if_exists='append', index=False)

if __name__ == '__main__':
    main()
