# encoding: utf-8

# 在已经查找出具有时间连续特征的病人的基础上，求是否具有

import pandas as pd
import json
from datetime import timedelta

# #查看有几家医院
data_cz = pd.read_csv(u'./data/城职.csv',encoding='gb18030')
data_cx = pd.read_csv(u'./data/城乡.csv', encoding='gb18030')
data = pd.concat((data_cz, data_cx), axis=0)
data_1 = data.rename(columns={u'就诊登记号': 'hos_id', u'医院编码': 'hos_ma', u'医院名称': 'hos_name', u'入院时间': 'in_time', u'出院时间': 'out_time', u'第一出院诊断疾病编码': 'disease_id',u'第一出院诊断': 'disease_name', u'患者姓名': 'person_name', u'患者身份证': 'person_id'})
# data_1 = data_1[['hos_id','hos_ma','hos_name','in_time','out_time','disease_id','disease_name','person_name','person_id']]
# print len(data_1)
#TODO 过滤看病次数小于3次的患者
pidCount = data_1['hos_id'].groupby(data_1['person_id']).count().reset_index()
pidlist = pidCount[pidCount['hos_id'] > 2]['person_id']
data_1 = data_1[data_1.person_id.isin(pidlist)]

result = data_1[data_1['person_id'].isin(['51010319321201004','510102193402114708'])]
result.to_csv('./data/ybxx/reslt_95006_01.csv',encoding='gb18030')

# hosCount = data_1['hos_id'].groupby(data_1['hos_ma']).count().reset_index()
# hosCount.sort_values(by=['hos_id'], ascending=[False], inplace=True)
# hoslist = list(hosCount[hosCount['hos_id'] > 100]['hos_ma'])
# print len(hoslist)
# hosCount.sort_values(by=['hos_id'],ascending=[False], inplace=True)
# hosCount_1 = hosCount.reset_index()
# hoslist = hosCount_1[hosCount_1['hos_id']>100]['hos_ma']
# print(hoslist)

