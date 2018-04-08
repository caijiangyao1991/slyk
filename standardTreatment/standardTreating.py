# -*- coding: utf-8 -*-
from tool.DBAcess.postgre import DBAccess
import pandas as pd
import numpy as np
import psycopg2
import os
import logging
from configparser import ConfigParser
import fp_growth
from matplotlib import pyplot as plt
import FileDialog
plt.rcParams['font.sans-serif'] = ['SimHei']  #用来显示正常的中文标签

import sys
reload(sys)
sys.setdefaultencoding('utf8')


class logger():
    def __init__(self, path):
        self.path = path
        self.logger = self.__pz(self.path)
    #封装的函数，外部不能调用
    def __pz(self,path):
        path1 = path + u'\log.log'
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                            filename='%s' % path1,
                            filemode='w+')
        #创建logger
        logger_name = 'standardTreating'
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)

        #create stream handler
        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        # create format
        fmt = "%(asctime)-15s %(levelname)s %(filename)s %(lineno)d  %(message)s"
        datefmt = "%a %d %b %Y %H:%M:%S"
        formatter = logging.Formatter(fmt, datefmt)
        # add handler and formatter to logger
        sh.setFormatter(formatter)
        logging.getLogger('').addHandler(sh)
        return logger

def getdata(host, port, user, password, database):
    con = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
    sql = """SELECT KB01_CKB519, to_char(KC21_CKC538,'yyyy') as out_time, KC22_AAZ217,KC21_CKC546,KC21_AKC196, KC22_CKE521, KC22_CKC526, KA20_AKE001, KA20_AKE002,to_date(KC21_CKC538, 'yyyy-mm-dd')-to_date(KC21_CKC537,'yyyy-mm-dd') as days,SKC17_nl,kb01_aka101_name FROM "public"."t_jsxx" where KC21_JZ_NAME='住院' and KA20_AKE003='1' and substr(KC21_AKC196,1,3)='J44' and to_char(KC21_CKC538,'yyyy')='2016' order by kc22_aaz217
    """
    x = pd.read_sql(sql, con)
    return x


def flat(df):
    return reduce(lambda x, y: str(x) + ',' + str(y), df)

def getitems(df, id ,value):
    items = df[value].groupby(df[id]).apply(flat)
    items_1 = items.astype(str)
    lists = []
    for i in range(len(items_1) - 1):
        if len(items_1[i]) > 2:
            lists.append(items_1[i])
    return lists

# 为结果存放在当前路径下新建一个目录
def mkdir(path, log):
    import os
    # 判断路径是否存在
    # 存在     True
    # 不存在   False
    isExists = os.path.exists(path)
    #判断结果
    if not isExists:
        # 如果不存在则创建目录
        log.info('%s directory create success' %path)
        os.makedirs(path)
        return True
    else:
        # 如果目录存在则不创建，并提示目录已存在
        log.info('%s directory already exists' %path)
        return False


def  main():
    #TODO 获取当前目录
    path = os.path.split(os.path.realpath(__file__))[0]
    path.decode('gbk')
    #实例化
    logger1 = logger(path)
    #调用实例化里面的函数
    log = logger1.logger
    #TODO 开始运行
    log.info('run begin')

    #TODO 创建结果目录
    resultPath = path + u'\\result'
    mkdir(resultPath, log)

    #TODO 读取配置文件
    log.info('read configuration file begin')
    cf = ConfigParser()
    localPath = path + u'\\test.ini'
    cf.read(localPath)
    database = cf.get(u'db', 'database')
    password = cf.get(u'db', 'password')
    user = cf.get(u'db', 'user')
    port = cf.getint(u'db', 'port')
    host = cf.get(u'db', 'host')
    log.info('read configuration file end')

    # TODO 开始分析
    log.info('read data begin')
    x = getdata(host, port, user, password, database)
    path1 = path + u'\\drugloc.xlsx'
    path2 = path + u'\\druglocgroup.xlsx'
    drugloc = pd.read_excel(path1)
    druglocgroup = pd.read_excel(path2)
    log.info('Read data end')

    log.info('Begin analysis')
    hoslist = ['攀钢集团总医院密地院区','攀钢集团总医院长寿路院区','攀枝花市中心医院','中国十九冶集团有限公司职工医院','攀枝花煤业(集团)有限责任公司总医院','米易县人民医院','攀枝花市中西医结合医院','攀枝花市第二人民医院']
    fredegree = 0.2
    for hos in hoslist:
        log.info('this is hospital %s' % hos)
        xx = x[x['kb01_ckb519']== hos]
        # 构建药品组编码
        data = pd.merge(xx, drugloc, left_on='ka20_ake001', right_on='drugId', how='left')
        data_1 = pd.merge(data, druglocgroup, left_on='locId', right_on='locId', how='left')
        # 去除druggroupid为空值的
        data_2 = data_1[(data_1['drugGroupId'].isnull() == False) & (data_1['kc22_aaz217'].isnull() == False)]
        data_3 = data_2[['kc22_aaz217', 'drugGroupId']]
        data_4 = data_3.drop_duplicates()
        #找到频繁药品
        druglist = getitems(data_4, 'kc22_aaz217', 'drugGroupId')
        transactions = [line.split(',') for line in druglist]
        itemsets = list(fp_growth.find_frequent_itemsets(transactions, fredegree* len(transactions)))
        # 去掉重复交换的频繁项即(a,b),(b,a)
        for i in range(len(itemsets)):
            for j in range(i + 1, len(itemsets)):
                if j < len(itemsets):
                    if (len(itemsets[i]) > 1) & (set(itemsets[i]) == set(itemsets[j])):
                        itemsets.remove(itemsets[i])
        # 去掉频繁1项集
        table = []
        num = []
        for items in itemsets:
            if len(items) > 1:
                table.append(items)
                num.append(len(items))
        freitem = pd.DataFrame({'fre': table, 'num': num})
        freitem.sort_values(by=['num'], ascending=[False], inplace=True)
        maxfre = max(freitem['num'])
        # 找到频繁n项集，中各项之间的交集或并集
        freitems = freitem[freitem['num'] == maxfre]
        unionfre = set(freitems.iloc[0, 0]).union(*freitems.iloc[1:, 0])

        # 计算权重 日均使用频率，平均花费,费用比率等
        # 计算日均使用频率
        drugcount = data_2['kc22_aaz217'].groupby([data_2['kc22_aaz217'], data_2['drugGroupId']]).count().reset_index()
        drugcount.rename(columns={0: 'count'}, inplace=True)
        days = data_2[['kc22_aaz217', 'days']].drop_duplicates()
        days.replace(0, 1, inplace=True)
        drug = pd.merge(drugcount, days, left_on='kc22_aaz217', right_on='kc22_aaz217', how='left')
        drug['meanCount'] = drug['count'] / drug['days']
        drugmeandays = drug['meanCount'].groupby(drug['drugGroupId']).mean().reset_index()
        # 计算平均花费
        drugcost = data_2['kc22_ckc526'].groupby([data_2['kc22_aaz217'], data_2['drugGroupId']]).sum().reset_index()
        drugcost.rename(columns={'kc22_ckc526': 'cost'}, inplace=True)
        drugmeancost = drugcost['cost'].groupby(drug['drugGroupId']).mean().reset_index()
        # 计算平均药品价格
        drugprice = data_2['kc22_cke521'].groupby(data_2['drugGroupId']).mean().reset_index()
        drugprice.rename(columns={'kc22_cke521': 'price'}, inplace=True)
        # 计算费用比率
        sumcost = data_2['kc22_ckc526'].groupby(data_2['kc22_aaz217']).sum().reset_index()
        sumcost.rename(columns={'kc22_ckc526': 'sumcost'}, inplace=True)
        drugratio = pd.merge(drugcost, sumcost, left_on='kc22_aaz217', right_on='kc22_aaz217', how='left')
        drugratio['ratio'] = drugratio['cost'] / drugratio['sumcost']
        ratio = drugratio['ratio'].groupby(drugratio['drugGroupId']).mean().reset_index()

        # 查看这几个药品组中对应了几个药，
        druggroup = data_2['kc22_cke521'].groupby(
            [data_2['drugGroupId'], data_2['drugGroupName'], data_2['ka20_ake002']]).mean().reset_index()
        drugnum = druggroup['ka20_ake002'].groupby(druggroup['drugGroupId']).count().reset_index()
        drugnum.rename(columns={0: 'drugnum'}, inplace=True)
        # 计算最高价格的是哪个药品
        druggroup.sort_values(by=['kc22_cke521'], ascending=[False], inplace=True)
        drugmaxprice = druggroup.drop_duplicates(subset=['drugGroupId'], keep='first')
        # 计算频率出现最高的是哪个药
        drugmaxfre = data_2['kc22_aaz217'].groupby(
            [data_2['drugGroupId'], data_2['drugGroupName'], data_2['ka20_ake002']]).count().reset_index()
        drugmaxfre.rename(columns={0: 'drugmaxfre'}, inplace=True)
        drugmaxfre.sort_values(by=['drugmaxfre'], ascending=[False], inplace=True)
        drugmaxfre = drugmaxfre.drop_duplicates(subset=['drugmaxfre'], keep='first')

        drugname = []
        meandayfre = []
        meancost = []
        meanprice = []
        costratio = []
        drugnums = []
        drugpricemax = []
        drugfremax = []
        for i in unionfre:
            drugname.append(list(data_2[data_2['drugGroupId'] == i]['drugGroupName'])[0])
            meandayfre.append(list(drugmeandays[drugmeandays['drugGroupId'] == i]['meanCount'])[0])
            meancost.append(list(drugmeancost[drugmeancost['drugGroupId'] == i]['cost'])[0])
            meanprice.append(list(drugprice[drugprice['drugGroupId'] == i]['price'])[0])
            costratio.append(list(ratio[ratio['drugGroupId'] == i]['ratio'])[0])
            drugnums.append(list(drugnum[drugnum['drugGroupId'] == i]['drugnum'])[0])
            drugpricemax.append(list(drugmaxprice[drugmaxprice['drugGroupId'] == i]['ka20_ake002'])[0])
            drugfremax.append(list(drugmaxfre[drugmaxfre['drugGroupId'] == i]['ka20_ake002'])[0])
        data = {'drugname': drugname, 'meandayfre': meandayfre, 'meancost': meancost, 'meanprice': meanprice,
                    'costratio': costratio, 'drugnums': drugnums, 'drugpricemax': drugpricemax,
                    'drugfremax': drugfremax}
        df = pd.DataFrame(data, columns=['drugname', 'meandayfre', 'meancost', 'meanprice', 'costratio', 'drugnums',
                                         'drugpricemax', 'drugfremax'])

        pathfile = resultPath + '\\result_'+ hos+'.csv'
        log.info('The result is saved in %s' % pathfile)
        df.to_csv(pathfile, encoding='gbk')
    log.info('End analysis')
    log.info('run end')



if __name__ == '__main__':
    main()
